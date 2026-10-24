//! 无 .proto 的 protobuf 通用解析。
//!
//! 游戏没有公开协议定义，字段号是逆向归纳的（见 Genshin/artifacts/CHAT-API-HANDOFF.md）。
//! 因此不能依赖生成的代码，只能按 wire format 逐字段读取。

use anyhow::{Result, bail};

/// 一个已解析的 protobuf 字段值。
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Varint(u64),
    Bytes(Vec<u8>),
    Fixed64(u64),
    Fixed32(u32),
}

/// 解析后的消息：字段号 → 该字段出现过的所有值（protobuf 允许重复）。
#[derive(Clone, Debug, Default)]
pub struct Fields {
    values: Vec<(u32, Value)>,
}

impl Fields {
    /// 取第一个 varint 字段。
    pub fn varint(&self, field: u32) -> Option<u64> {
        self.values.iter().find_map(|(f, v)| match (f, v) {
            (f, Value::Varint(n)) if *f == field => Some(*n),
            _ => None,
        })
    }

    /// 取第一个字节字段。
    pub fn bytes(&self, field: u32) -> Option<&'_[u8]> {
        self.values.iter().find_map(|(f, v)| match (f, v) {
            (f, Value::Bytes(b)) if *f == field => Some(b.as_slice()),
            _ => None,
        })
    }

    /// 取第一个字节字段并按 UTF-8 解析；非 UTF-8 时返回 None。
    pub fn string(&self, field: u32) -> Option<&str> {
        self.bytes(field).and_then(|b| std::str::from_utf8(b).ok())
    }

    /// 取第一个字节字段并当作嵌套消息解析。
    pub fn message(&self, field: u32) -> Option<Fields> {
        self.bytes(field).and_then(|b| parse(b).ok())
    }

    /// 该字段出现过的所有字节值（用于 repeated 嵌套消息）。
    pub fn repeated_messages(&self, field: u32) -> Vec<Fields> {
        self.values
            .iter()
            .filter_map(|(f, v)| match (f, v) {
                (f, Value::Bytes(b)) if *f == field => parse(b).ok(),
                _ => None,
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// 读取 varint。
fn read_varint(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *buf.get(*pos).ok_or_else(|| anyhow::anyhow!("varint 越界"))?;
        *pos += 1;
        result |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            bail!("varint 过长");
        }
    }
}

/// 解析一段 protobuf 字节。
///
/// 注意：字符串与嵌套消息在 wire format 上不可区分。这里**不做启发式猜测**——
/// wire type 2 一律存为 `Bytes`，由调用方决定按字符串还是嵌套消息解释。
/// 这比"先试嵌套、失败再当 UTF-8"可靠：不会把恰好能解析的文本误判成消息。
pub fn parse(buf: &[u8]) -> Result<Fields> {
    let mut fields = Fields::default();
    let mut pos = 0usize;
    while pos < buf.len() {
        let tag = read_varint(buf, &mut pos)?;
        let field = u32::try_from(tag >> 3).map_err(|_| anyhow::anyhow!("字段号过大"))?;
        if field == 0 {
            bail!("字段号 0 非法");
        }
        let value = match tag & 7 {
            0 => Value::Varint(read_varint(buf, &mut pos)?),
            1 => {
                let end = pos.checked_add(8).ok_or_else(|| anyhow::anyhow!("长度溢出"))?;
                let slice = buf.get(pos..end).ok_or_else(|| anyhow::anyhow!("fixed64 越界"))?;
                pos = end;
                Value::Fixed64(u64::from_le_bytes(slice.try_into().expect("8 字节")))
            }
            2 => {
                let len = usize::try_from(read_varint(buf, &mut pos)?)
                    .map_err(|_| anyhow::anyhow!("长度过大"))?;
                let end = pos.checked_add(len).ok_or_else(|| anyhow::anyhow!("长度溢出"))?;
                let slice = buf.get(pos..end).ok_or_else(|| anyhow::anyhow!("bytes 越界"))?;
                pos = end;
                Value::Bytes(slice.to_vec())
            }
            5 => {
                let end = pos.checked_add(4).ok_or_else(|| anyhow::anyhow!("长度溢出"))?;
                let slice = buf.get(pos..end).ok_or_else(|| anyhow::anyhow!("fixed32 越界"))?;
                pos = end;
                Value::Fixed32(u32::from_le_bytes(slice.try_into().expect("4 字节")))
            }
            other => bail!("不支持的 wire type {other}"),
        };
        fields.values.push((field, value));
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_varint_and_bytes() {
        let buf = [0x08, 0x96, 0x01, 0x12, 0x02, b'h', b'i'];
        let f = parse(&buf).expect("解析");
        assert_eq!(f.varint(1), Some(150));
        assert_eq!(f.string(2), Some("hi"));
    }

    #[test]
    fn parses_nested_message() {
        let buf = [0x1a, 0x02, 0x08, 0x07];
        let f = parse(&buf).expect("解析");
        let nested = f.message(3).expect("嵌套");
        assert_eq!(nested.varint(1), Some(7));
    }

    #[test]
    fn rejects_truncated_input() {
        assert!(parse(&[0x12, 0x05, b'a']).is_err());
    }

    #[test]
    fn rejects_zero_field_number() {
        assert!(parse(&[0x00, 0x01]).is_err());
    }
}