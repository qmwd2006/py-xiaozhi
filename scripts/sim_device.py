#!/usr/bin/env python3
"""无界面小智模拟设备（调试用）。

复用 py-xiaozhi 的 OTA / WebSocket 协议 / MCP 服务，但不加载 TUI、音频、唤醒词插件，
因此可以在无麦克风、无扬声器、无图形终端的环境下运行。

用法：
    .venv/bin/python scripts/sim_device.py
    .venv/bin/python scripts/sim_device.py --text "播放烟花易冷"
    .venv/bin/python scripts/sim_device.py --interactive
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault("XIAOZHI_DISABLE_AUDIO", "1")

from src.activation.identity import DeviceIdentity  # noqa: E402
from src.activation.ota import OtaConfigClient  # noqa: E402
from src.constants.constants import ListeningMode  # noqa: E402
from src.core.event_bus import EventBus, Events  # noqa: E402
from src.core.protocol_manager import ProtocolManager  # noqa: E402
from src.logging import get_logger  # noqa: E402
from src.mcp.mcp_server import McpServer  # noqa: E402
from src.utils.config_manager import ConfigManager  # noqa: E402

logger = get_logger()

MCP_METHODS = {"initialize", "tools/list", "tools/call"}


class SimDevice:
    def __init__(self, config: ConfigManager) -> None:
        self.config = config
        self.bus = EventBus()
        self.protocol = ProtocolManager(self.bus, config=config)
        self.server = McpServer(config=config)
        self._mcp_ready = asyncio.Event()

    async def prepare(self) -> bool:
        # 1) OTA 拉取网络配置
        identity = DeviceIdentity()
        identity.init_paths()
        identity.ensure_efuse_file()
        ota = OtaConfigClient(self.config, identity)
        await ota.ensure_local_ip()
        ota.initialize_config()
        await ota.fetch_ota_config()
        logger.info(
            "设备身份 DEVICE_ID=%s CLIENT_ID=%s",
            self.config.get_config("SYSTEM_OPTIONS.DEVICE_ID"),
            self.config.get_config("SYSTEM_OPTIONS.CLIENT_ID"),
        )

        # 2) 注册 MCP 工具（内置 + 外挂探针）
        self.server.register_builtin_tools(music_player=None)
        loaded = self.server.reload_external_plugins()
        logger.info(
            "MCP 工具: %s", [t.name for t in self.server.tools]
        )
        logger.info("外挂插件: %s", [(p.plugin_id, p.tool_names) for p in loaded])

        # 3) 绑定 MCP 发送回调 -> 走协议
        async def _send(payload: str) -> None:
            await self.protocol.send_mcp_message(payload)

        self.server.set_send_callback(_send)

        # 4) 订阅入站 JSON
        self.bus.on(Events.INCOMING_JSON, self._on_incoming_json)
        return True

    async def _on_incoming_json(self, data) -> None:
        if not isinstance(data, dict):
            return
        msg_type = data.get("type")
        if msg_type == "mcp":
            payload = data.get("payload")
            if isinstance(payload, dict):
                method = payload.get("method")
                logger.info("[MCP<<] %s id=%s", method, payload.get("id"))
                await self.server.parse_message(payload)
            return
        if msg_type == "tts":
            state = data.get("state")
            text = data.get("text")
            if state == "sentence_start" and text:
                logger.info("[TTS] %s", text)
            elif state == "start":
                logger.info("[TTS] start")
            elif state == "stop":
                logger.info("[TTS] stop")
            return
        if msg_type == "stt":
            logger.info("[STT] %s", data.get("text"))
            return
        if msg_type == "llm":
            logger.info("[LLM] %s", data.get("text"))
            return
        logger.debug("[JSON] %s", json.dumps(data, ensure_ascii=False)[:400])

    async def connect(self) -> bool:
        self.protocol.set_protocol("websocket")
        self.protocol.set_audio_handler(None)
        if not await self.protocol.connect():
            return False
        logger.info("已连接官方云")
        return True

    async def send_text(self, text: str) -> None:
        logger.info("[SEND] %s", text)
        await self.protocol.send_start_listening(ListeningMode.MANUAL)
        await self.protocol.send_wake_word_detected(text)
        await asyncio.sleep(0.2)
        await self.protocol.send_stop_listening()

    async def run(self, texts: list[str], interactive: bool) -> None:
        for text in texts:
            await self.send_text(text)
            # 等待本轮响应（TTS stop 或超时）
            await asyncio.sleep(20)

        if interactive:
            loop = asyncio.get_running_loop()
            while True:
                try:
                    line = await loop.run_in_executor(None, input, "你> ")
                except (EOFError, KeyboardInterrupt):
                    break
                line = (line or "").strip()
                if not line:
                    continue
                if line in ("q", "quit", "exit"):
                    break
                await self.send_text(line)
        else:
            await asyncio.sleep(5)


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--text", action="append", default=[], help="要发送的文本，可多次")
    parser.add_argument("--interactive", action="store_true", help="进入交互输入")
    parser.add_argument("--duration", type=int, default=60, help="无文本时的保持时长(秒)")
    args = parser.parse_args()

    config = ConfigManager()
    config.update_config("SYSTEM_OPTIONS.NETWORK.ACTIVATION_VERSION", "v1")
    config.update_config("WAKE_WORD_OPTIONS.USE_WAKE_WORD", False)

    device = SimDevice(config)
    if not await device.prepare():
        return 1
    if not await device.connect():
        logger.error("连接失败")
        return 1

    try:
        if args.text or args.interactive:
            await device.run(args.text, args.interactive)
        else:
            logger.info("保持连接 %d 秒（无输入）", args.duration)
            await asyncio.sleep(args.duration)
    finally:
        await device.protocol.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
