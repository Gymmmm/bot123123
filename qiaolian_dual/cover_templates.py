"""Shared geometry and visual themes for the independent listing renderer."""
from __future__ import annotations

from dataclasses import dataclass


CANVAS_SIZE = (1200, 900)
WHITE_BORDER = 36
SAFE_INSET = 64
PHOTO_BOX = (WHITE_BORDER, WHITE_BORDER, CANVAS_SIZE[0] - WHITE_BORDER, CANVAS_SIZE[1] - WHITE_BORDER)
COVER_TEXT_BOX = (SAFE_INSET, 650, CANVAS_SIZE[0] - SAFE_INSET, CANVAS_SIZE[1] - SAFE_INSET)


@dataclass(frozen=True)
class CoverTemplate:
    key: str
    name: str
    panel: tuple[int, int, int, int]
    primary: tuple[int, int, int]
    secondary: tuple[int, int, int]
    accent: tuple[int, int, int]
    opacity: int


TEMPLATES = {
    "A": CoverTemplate("A", "精品地产杂志风", (247, 245, 239, 255), (31, 37, 41), (88, 92, 92), (151, 119, 68), 238),
    "B": CoverTemplate("B", "高端暗金风", (25, 25, 23, 255), (244, 239, 224), (198, 192, 177), (194, 153, 82), 238),
    "C": CoverTemplate("C", "建筑编辑风", (244, 246, 247, 255), (20, 44, 58), (83, 100, 109), (40, 116, 142), 242),
    "D": CoverTemplate("D", "温暖生活方式风", (250, 241, 231, 255), (73, 48, 39), (125, 91, 76), (191, 111, 77), 240),
}


def get_template(key: str) -> CoverTemplate:
    normalized = str(key or "A").strip().upper()
    if normalized not in TEMPLATES:
        raise ValueError(f"unknown_template:{normalized}")
    return TEMPLATES[normalized]


__all__ = ["CANVAS_SIZE", "WHITE_BORDER", "SAFE_INSET", "PHOTO_BOX", "COVER_TEXT_BOX", "CoverTemplate", "TEMPLATES", "get_template"]
