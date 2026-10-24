//! 与 YaeAchievement DLL 的管道协议。
//!
//! 协议来源：YaeAchievementLib/src/Goshujin.cs（DLL 侧）与
//! YaeAchievement/src/Utils.cs（宿主侧）。
//!
//! 帧格式：1 字节类型 + 各类型自己的载荷。
//! 这个模块**不依赖 Windows API**，因此可以在 Linux 上跑完整单测——
//! 协议错位曾导致游戏卡死，必须有穷举测试兜住。

use anyhow::Result;



/// 单包载荷上限（1 MiB）。真实聊天包远小于这个值。
pub const MAX_PAYLOAD: usize = 1024 * 1024;

/// 管道里 DLL 发来的一帧。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// 0x03 PushPlayerProp: type(i32) + 值(f64)。
    PlayerProp { kind: i32 },
    /// 0x04 PushPacketData: cmdId(u16) + 长度(u32) + 载荷。
    Packet { cmd: u16, payload: Vec<u8> },
    /// 0xFA 请求 cmdId 列表。
    LoadTasks,
    /// 0xFB 请求玩家属性列表。
    LoadProps,
    /// 0xFD 请求方法 RVA 表。
    LoadMethodTable,
    /// 0xFE 恢复游戏主线程。
    ResumeMainThread,
    /// 0xFF 退出。
    Exit,
}

/// 解析一帧。返回 `Ok(None)` 表示字节不够、需要继续读。
pub fn parse(kind: u8, body: &[u8]) -> Result<Option<(Frame, usize)>> {
    match kind {
        0x03 => {
            if body.len() < 12 {
                return Ok(None);
            }
            let kind = i32::from_le_bytes([body[0], body[1], body[2], body[3]]);
            Ok(Some((Frame::PlayerProp { kind }, 12)))
        }
        0x04 => {
            if body.len() < 6 {
                return Ok(None);
            }
            let cmd = u16::from_le_bytes([body[0], body[1]]);
            let len = u32::from_le_bytes([body[2], body[3], body[4], body[5]]) as usize;
            if len > MAX_PAYLOAD {
                anyhow::bail!("包体长度 {len} 超出上限，疑似流错位");
            }
            let total = 6 + len;
            if body.len() < total {
                return Ok(None);
            }
            Ok(Some((
                Frame::Packet {
                    cmd,
                    payload: body[6..total].to_vec(),
                },
                total,
            )))
        }
        0xFA => Ok(Some((Frame::LoadTasks, 0))),
        0xFB => Ok(Some((Frame::LoadProps, 0))),
        0xFD => Ok(Some((Frame::LoadMethodTable, 0))),
        0xFE => Ok(Some((Frame::ResumeMainThread, 0))),
        0xFF => Ok(Some((Frame::Exit, 0))),
        other => anyhow::bail!("未知管道消息 0x{other:02X}"),
    }
}

/// 对一帧的应答字节。
pub fn reply(frame: &Frame, rva: &crate::metadata::MethodRva) -> Vec<u8> {
    match frame {
        Frame::LoadTasks => {
            // 订阅信封 2246 与独立私聊 486，两者都可能承载聊天。
            let mut out = Vec::new();
            for cmd in crate::extract::WANTED_CMDS {
                out.extend(u32::from(*cmd).to_le_bytes());
            }
            out.extend(u32::MAX.to_le_bytes());
            out
        }
        Frame::LoadProps => u32::MAX.to_le_bytes().to_vec(),
        Frame::LoadMethodTable => {
            let mut out = Vec::new();
            for value in rva.to_wire() {
                out.extend(value.to_le_bytes());
            }
            out
        }
        Frame::PlayerProp { .. } | Frame::Packet { .. } | Frame::Exit => vec![0],
        Frame::ResumeMainThread => Vec::new(),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::MethodRva;

    fn sample_rva() -> MethodRva {
        MethodRva {
            do_cmd: 1,
            update_normal_prop: 2,
            new_string: 3,
            find_game_object: 4,
            event_system_update: 5,
            simulate_pointer_click: 6,
            to_int32: 425_587_504,
            tcp_state_ptr: 91_775_080,
            shared_info_ptr: 91_775_072,
            decompress: 115_234_864,
        }
    }

    #[test]
    fn every_known_frame_parses() {
        for (kind, expected) in [
            (0xFAu8, Frame::LoadTasks),
            (0xFB, Frame::LoadProps),
            (0xFD, Frame::LoadMethodTable),
            (0xFE, Frame::ResumeMainThread),
            (0xFF, Frame::Exit),
        ] {
            let (frame, used) = parse(kind, &[])
                .unwrap_or_else(|e| panic!("0x{kind:02X} 应可解析: {e}"))
                .unwrap_or_else(|| panic!("0x{kind:02X} 应立即可解析"));
            assert_eq!(frame, expected);
            assert_eq!(used, 0);
        }
    }

    #[test]
    fn player_prop_frame_consumes_12_bytes() {
        // 这个分支曾缺失，导致后续字节全被误读、DLL 等回执、游戏卡死。
        let mut body = 7i32.to_le_bytes().to_vec();
        body.extend(1.5f64.to_le_bytes());
        let (frame, used) = parse(0x03, &body).unwrap().unwrap();
        assert_eq!(frame, Frame::PlayerProp { kind: 7 });
        assert_eq!(used, 12);
    }

    #[test]
    fn packet_frame_reads_header_and_payload() {
        let mut body = 2246u16.to_le_bytes().to_vec();
        body.extend(3u32.to_le_bytes());
        body.extend([0xAA, 0xBB, 0xCC]);
        let (frame, used) = parse(0x04, &body).unwrap().unwrap();
        assert_eq!(frame, Frame::Packet { cmd: 2246, payload: vec![0xAA, 0xBB, 0xCC] });
        assert_eq!(used, 9);
    }

    #[test]
    fn partial_frames_ask_for_more_bytes() {
        assert!(parse(0x04, &[0x01, 0x02]).unwrap().is_none());
        assert!(parse(0x03, &[0; 11]).unwrap().is_none());
        let mut short = 2246u16.to_le_bytes().to_vec();
        short.extend(10u32.to_le_bytes());
        short.extend([0u8; 5]);
        assert!(parse(0x04, &short).unwrap().is_none());
    }

    #[test]
    fn absurd_length_is_rejected_instead_of_allocating() {
        let mut body = 2246u16.to_le_bytes().to_vec();
        body.extend(u32::MAX.to_le_bytes());
        let err = parse(0x04, &body).expect_err("超大长度必须拒绝");
        assert!(err.to_string().contains("流错位"), "{err}");
    }

    #[test]
    fn unknown_frame_is_an_error() {
        assert!(parse(0x24, &[0; 32]).is_err());
    }

    #[test]
    fn load_tasks_reply_subscribes_envelope_and_direct_private() {
        let reply = reply(&Frame::LoadTasks, &sample_rva());
        let mut expected = Vec::new();
        for cmd in crate::extract::WANTED_CMDS {
            expected.extend(u32::from(*cmd).to_le_bytes());
        }
        expected.extend(u32::MAX.to_le_bytes());
        assert_eq!(reply, expected);
        // 私聊是独立 cmd 486；只订阅 2246 会收不到好友消息。
        assert!(crate::extract::WANTED_CMDS.contains(&486));
        assert!(crate::extract::WANTED_CMDS.contains(&2246));
    }

    #[test]
    fn load_props_reply_is_immediate_terminator() {
        assert_eq!(reply(&Frame::LoadProps, &sample_rva()), u32::MAX.to_le_bytes());
    }

    #[test]
    fn method_table_reply_has_ten_u32_in_dll_order() {
        let reply = reply(&Frame::LoadMethodTable, &sample_rva());
        assert_eq!(reply.len(), 40, "DLL 读 10 个 u32");
        let values: Vec<u32> = reply
            .chunks_exact(4)
            .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(values[0], 1, "do_cmd 第一个读");
        assert_eq!(values[6], 425_587_504, "to_int32 第七个读");
        assert_eq!(values[7], 91_775_080, "tcp_state_ptr 第八个读");
        assert_eq!(values[9], 115_234_864, "decompress 最后读");
    }

    #[test]
    fn frames_needing_bool_ack_all_reply_one_byte() {
        let rva = sample_rva();
        for frame in [
            Frame::PlayerProp { kind: 1 },
            Frame::Packet { cmd: 2246, payload: vec![] },
            Frame::Exit,
        ] {
            assert_eq!(reply(&frame, &rva).len(), 1, "{frame:?} 需要一个 bool");
        }
        assert!(reply(&Frame::ResumeMainThread, &rva).is_empty());
    }

    #[test]
    fn full_handshake_sequence_is_consumed_exactly() {
        let mut stream = vec![0xFA, 0xFB, 0xFD, 0xFE, 0x04];
        stream.extend(2246u16.to_le_bytes());
        stream.extend(4u32.to_le_bytes());
        stream.extend([1, 2, 3, 4]);

        let mut offset = 0usize;
        let mut seen = Vec::new();
        while offset < stream.len() {
            let kind = stream[offset];
            offset += 1;
            let (frame, used) = parse(kind, &stream[offset..])
                .unwrap()
                .expect("完整流里不应出现半帧");
            offset += used;
            seen.push(frame);
        }
        assert_eq!(
            seen,
            vec![
                Frame::LoadTasks,
                Frame::LoadProps,
                Frame::LoadMethodTable,
                Frame::ResumeMainThread,
                Frame::Packet { cmd: 2246, payload: vec![1, 2, 3, 4] },
            ],
            "握手 + 首包必须被精确消费"
        );
        assert_eq!(offset, stream.len(), "整条流应正好读完");
    }
}