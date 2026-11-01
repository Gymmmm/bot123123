"""Read source overlay text without cropping or repainting property evidence."""
from __future__ import annotations

import csv
import io
import os
import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from PIL import Image, ImageOps

_CONTACT = re.compile(r"(?i)(?:\btel(?:ephone)?\b|\bphone\b|\bwhats\s*app\b|\btelegram\b|\bwechat\b|\bline\s*id\b|\breal\s*estate\b|\brealtor\b|\bt\.me\b|\bwww\.|微信|电话|联系|中介|地产|房产)")
_PHONE = re.compile(r"(?<!\d)(?:\+?\d[ \-().]*){7,15}(?!\d)")


def detect_source_overlay(path: str | Path) -> dict[str, object]:
    binary = shutil.which("tesseract")
    if not binary:
        raise RuntimeError("source_overlay_verifier_unavailable")
    with tempfile.TemporaryDirectory(prefix="qiaolian-overlay-") as temp:
        normalized = Path(temp) / "source.png"
        with Image.open(path) as original:
            frame = ImageOps.exif_transpose(original).convert("RGB")
            frame.thumbnail((1800,1800))
            frame.save(normalized)
        completed = subprocess.run(
            [binary,str(normalized),"stdout","-l","eng+chi_sim","--psm","11","tsv"],
            capture_output=True,text=True,timeout=25,check=True,
            env={**os.environ, "OMP_THREAD_LIMIT":"1"},
        )
    lines: dict[tuple[str,...], list[str]] = {}
    for row in csv.DictReader(io.StringIO(completed.stdout), delimiter="\t"):
        try:
            if float(row.get("conf") or -1) < 45:
                continue
        except ValueError:
            continue
        word = (row.get("text") or "").strip()
        if word:
            key = tuple(row.get(k,"") for k in ("block_num","par_num","line_num"))
            lines.setdefault(key,[]).append(word)
    for words in lines.values():
        text = " ".join(words)
        if _CONTACT.search(text) or _CONTACT.search(text.replace(" ","")) or _PHONE.search(text):
            return {"blocked":True,"reason":"source_contact_or_brand_text"}
    return {"blocked":False,"reason":"no_contact_text_detected"}
