# Miliastra-Wonderland-ChatBridge

用于本地消息接入与应用集成的辅助工具，提供 HTTP / WebSocket 接口。

## 构建

宿主使用 Rust，原生组件使用 .NET 9。完整构建需要 Windows x64 及相应的 MSVC / Windows SDK 工具链。

```sh
cargo build --release
dotnet publish native/MiliastraWonderlandChatBridge.Native.csproj -c Release
```

## 使用

从 GitHub Releases 下载 Windows x64 压缩包并解压，使用独立端口和自定义令牌启动。包内包含宿主、原生组件和版本相关的 `metadata.bin`；游戏更新后可能需要更新该资源。启动前退出已有游戏进程。

```sh
miliastra-wonderland-chatbridge --port 18889 --token <自定义令牌> --self-uid <UID>
```

服务监听本机地址。请求使用 `x-miliastra-token` 鉴权，提供 `/health`、`/messages`、`/names` 和 `/stream` 接口。使用 `--help` 查看启动参数。

## 许可与致谢

本项目采用 **GNU GPL v3.0（GPL-3.0-only）**，完整条款见 [LICENSE](LICENSE)。第三方组件保留各自的版权与许可。

原生组件基于 [YaeAchievement](https://github.com/HolographicHat/Yae) 修改。原作者版权声明：Copyright (C) 2022–2024 HoloHat。修改包括消息接入、生命周期、资源管理与健壮性处理；上游原有的 GPL v3 或更高版本授权声明予以保留。

包含 [MinHook](https://github.com/TsudaKageyu/minhook)（作者 Tsuda Kageyu），采用 BSD-2-Clause，条款见 [THIRD_PARTY_LICENSES](THIRD_PARTY_LICENSES)。
