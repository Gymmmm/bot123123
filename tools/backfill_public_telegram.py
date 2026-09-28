#!/usr/bin/env python3
"""从 Telegram 公开频道预览补回 source_posts 原文和图片，无需用户会话。"""
from __future__ import annotations

import argparse
import html
import json
import re
import sqlite3
import time
import urllib.request
from html.parser import HTMLParser
from pathlib import Path


class _Text(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)


def _plain(fragment: str) -> str:
    parser = _Text()
    parser.feed(fragment.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n"))
    return html.unescape("".join(parser.parts)).strip()


def _fetch(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 QiaolianCollector/1.0"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode("utf-8", errors="replace")


def _message_block(page: str, channel: str, post_id: int) -> str:
    marker = f'data-post="{channel}/{post_id}"'
    pos = page.find(marker)
    if pos < 0:
        return ""
    start = page.rfind('<div class="tgme_widget_message_wrap', 0, pos)
    end = page.find('<div class="tgme_widget_message_wrap', pos + len(marker))
    return page[start : end if end >= 0 else len(page)] if start >= 0 else ""


def _extract(block: str) -> tuple[str, list[str]]:
    text = ""
    match = re.search(r'<div class="tgme_widget_message_text[^>]*>(.*?)</div>', block, flags=re.S)
    if match:
        text = _plain(match.group(1))
    urls = re.findall(r"background-image:url\('([^']+telesco\.pe/[^']+)'\)", block)
    return text, list(dict.fromkeys(html.unescape(url) for url in urls))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True)
    ap.add_argument("--channel", default="zufang555")
    ap.add_argument("--media-root", default="media/collector_downloads")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, source_post_id, source_url, raw_text FROM source_posts WHERE source_name=? ORDER BY id",
        (args.channel,),
    ).fetchall()
    if args.limit > 0:
        rows = rows[: args.limit]
    root = Path(args.media_root) / args.channel
    root.mkdir(parents=True, exist_ok=True)
    ok = missing = images_total = 0
    for row in rows:
        # 历史 CSV 里 source_post_id 是旧库行号；真实 Telegram 消息号在 source_url 末尾。
        url_match = re.search(r"/(\d+)(?:[?#].*)?$", str(row["source_url"] or ""))
        post_id = int(url_match.group(1)) if url_match else int(row["source_post_id"])
        page = _fetch(f"https://t.me/s/{args.channel}?before={post_id + 1}")
        block = _message_block(page, args.channel, post_id)
        text, image_urls = _extract(block)
        if not block:
            missing += 1
            print(f"MISS {post_id}")
            continue
        post_dir = root / str(post_id)
        post_dir.mkdir(parents=True, exist_ok=True)
        images: list[dict] = []
        for index, url in enumerate(image_urls, start=1):
            target = post_dir / f"{post_id}_{index:02d}.jpg"
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urllib.request.urlopen(req, timeout=45) as response:
                    target.write_bytes(response.read())
                images.append({"local_path": str(target.resolve()), "source_url": url, "sort_order": index - 1})
            except Exception as exc:
                print(f"IMAGE_FAIL {post_id} {index}: {type(exc).__name__}")
        # 原文必须先经过现有 sanitizer/parser；此处仅恢复证据层，不直接发布。
        conn.execute(
            "UPDATE source_posts SET raw_text=?, raw_images_json=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (text or row["raw_text"], json.dumps(images, ensure_ascii=False), row["id"]),
        )
        conn.commit()
        ok += 1
        images_total += len(images)
        print(f"OK {post_id} text={len(text)} images={len(images)}")
        time.sleep(0.25)
    print(json.dumps({"ok": ok, "missing": missing, "images": images_total}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
