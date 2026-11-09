#!/usr/bin/env python3
"""语音控制流程自动化验证（基于 py-xiaozhi 模拟设备）。

对每条语音指令：发送 -> 等待本轮结束 -> 记录设备端被调用的 self.music.* 工具，
并与期望序列比对。

用法：
    .venv/bin/python scripts/verify_voice.py            # 跑全部用例
    .venv/bin/python scripts/verify_voice.py -k 暂停     # 只跑名字含「暂停」的用例
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import time
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


class Verifier:
    def __init__(self, config: ConfigManager) -> None:
        self.config = config
        self.bus = EventBus()
        self.protocol = ProtocolManager(self.bus, config=config)
        self.server = McpServer(config=config)
        self.device_tools: list[str] = []
        self.cloud_tools: list[str] = []
        self.tts_texts: list[str] = []
        self.turn_done = asyncio.Event()
        self.mcp_ready = asyncio.Event()

    async def prepare(self) -> bool:
        identity = DeviceIdentity()
        identity.init_paths()
        identity.ensure_efuse_file()
        ota = OtaConfigClient(self.config, identity)
        await ota.ensure_local_ip()
        ota.initialize_config()
        await ota.fetch_ota_config()

        self.server.register_builtin_tools(music_player=None)
        self.server.reload_external_plugins()

        async def _send(payload: str) -> None:
            await self.protocol.send_mcp_message(payload)

        self.server.set_send_callback(_send)
        self.bus.on(Events.INCOMING_JSON, self._on_incoming_json)
        return True

    async def _on_incoming_json(self, data) -> None:
        if not isinstance(data, dict):
            return
        msg_type = data.get("type")
        if msg_type == "mcp":
            payload = data.get("payload")
            if not isinstance(payload, dict):
                return
            method = payload.get("method")
            if method == "tools/call":
                params = payload.get("params") or {}
                name = params.get("name", "")
                args = params.get("arguments") or {}
                self.device_tools.append(name)
                logger.info("  [设备工具] %s %s", name, args)
            elif method == "tools/list":
                self.mcp_ready.set()
            await self.server.parse_message(payload)
            return
        if msg_type == "tts":
            state = data.get("state")
            text = data.get("text")
            if state == "sentence_start" and text:
                self.tts_texts.append(text)
                # 云端执行工具时会先播报「% 工具名...」
                m = re.match(r"^%\s*([a-zA-Z0-9_.]+?)(?:\.\.\.|\s|$)", text.strip())
                if m:
                    self.cloud_tools.append(m.group(1).rstrip("."))
            elif state == "stop":
                self.turn_done.set()
            return

    async def connect(self) -> bool:
        self.protocol.set_protocol("websocket")
        self.protocol.set_audio_handler(None)
        if not await self.protocol.connect():
            return False
        # 等待云端完成 MCP 握手（tools/list）
        try:
            await asyncio.wait_for(self.mcp_ready.wait(), timeout=15)
        except asyncio.TimeoutError:
            logger.warning("等待 MCP 握手超时，继续")
        return True

    async def run_turn(self, text: str, timeout: float = 45) -> tuple[list[str], list[str], list[str]]:
        self.device_tools = []
        self.cloud_tools = []
        self.tts_texts = []
        self.turn_done.clear()

        await self.protocol.send_start_listening(ListeningMode.MANUAL)
        await self.protocol.send_wake_word_detected(text)
        await asyncio.sleep(0.2)
        await self.protocol.send_stop_listening()

        try:
            await asyncio.wait_for(self.turn_done.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("本轮等待 TTS stop 超时（%.0fs）", timeout)
        # 给迟到的工具调用一点缓冲
        await asyncio.sleep(1.5)
        return list(self.device_tools), list(self.tts_texts), list(self.cloud_tools)


# (名称, 指令, 期望设备工具匹配器)
CASES = [
    ("单曲点播", "播放稻香", ["play_song", "self.music.play_url"]),
    ("多曲连播", "播放几首周杰伦的歌", ["play_playlist", "self.music.enqueue_url"]),
    ("曲库列表", "曲库里都有什么歌", ["library_list"]),
    ("曲库搜索", "放曲库里的李白", ["library_search", "self.music.play_url"]),
    ("曲库统计", "曲库占多大空间", ["library_stats"]),
    ("暂停", "暂停一下", ["self.music.pause"]),
    ("继续", "继续播放", ["self.music.resume"]),
    ("下一首", "下一首", ["self.music.next"]),
    ("上一首", "上一首", ["self.music.prev"]),
    ("快进", "快进三十秒", ["self.music.skip"]),
    ("跳转", "跳到一分三十秒", ["self.music.seek"]),
    ("单曲循环", "单曲循环", ["self.music.set_repeat"]),
    ("播放状态", "现在放的什么歌", ["self.music.status"]),
    ("停止", "停止播放", ["self.music.stop"]),
]


def check(expected: list[str], actual: list[str]) -> tuple[bool, str]:
    """期望的每个工具都应出现在实际调用序列中（顺序匹配，允许中间夹杂其它工具）。"""
    idx = 0
    for exp in expected:
        found = False
        while idx < len(actual):
            if actual[idx] == exp:
                found = True
                idx += 1
                break
            idx += 1
        if not found:
            return False, f"缺少 {exp}"
    return True, ""


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("-k", "--keyword", default="", help="只跑名称含该关键词的用例")
    args = parser.parse_args()

    config = ConfigManager()
    config.update_config("SYSTEM_OPTIONS.NETWORK.ACTIVATION_VERSION", "v1")
    config.update_config("WAKE_WORD_OPTIONS.USE_WAKE_WORD", False)

    cases = [c for c in CASES if not args.keyword or args.keyword in c[0]]
    if not cases:
        print(f"没有匹配「{args.keyword}」的用例")
        return 1

    v = Verifier(config)
    if not await v.prepare():
        return 1
    if not await v.connect():
        logger.error("连接失败")
        return 1

    results = []
    try:
        for name, text, expected in cases:
            print(f"\n{'='*60}\n[{name}] 指令: {text}\n{'='*60}")
            dev_tools, tts, cloud_tools = await v.run_turn(text)
            # 合并云端与设备端调用，按发生顺序近似（云端工具先于设备工具）
            merged = cloud_tools + dev_tools
            ok, reason = check(expected, merged)
            results.append((name, text, expected, merged, ok, reason, tts))
            print(f"  云端工具: {cloud_tools}")
            print(f"  设备工具: {dev_tools}")
            if tts:
                print(f"  回复: {' / '.join(tts[:2])}")
            print(f"  结果: {'✅ PASS' if ok else '❌ FAIL ' + reason}")
    finally:
        await v.protocol.disconnect()

    # 汇总
    print(f"\n\n{'='*60}\n汇总\n{'='*60}")
    passed = sum(1 for r in results if r[4])
    for name, text, expected, tools, ok, reason, tts in results:
        mark = "✅" if ok else "❌"
        print(f"{mark} {name:8} 期望={expected} 实际={tools}")
        if not ok:
            print(f"     原因: {reason} | 回复: {tts[:2]}")
    print(f"\n通过 {passed}/{len(results)}")
    return 0 if passed == len(results) else 2


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
