"""为同一套实拍快速生成三种手机端封面方向，仅用于审美选型。"""
from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps

from cover_generator import _font

W, H = 1280, 720
BLUE = (21, 83, 190, 245)
WHITE = (255, 255, 255, 255)


def _fit(draw, text, start, minimum, width):
    for size in range(start, minimum - 1, -2):
        font = _font(size, True)
        if draw.textbbox((0, 0), text, font=font)[2] <= width:
            return font
    return _font(minimum, True)


def _base(path: str):
    im = Image.open(path).convert("RGB")
    im = ImageOps.fit(im, (W, H), method=Image.Resampling.LANCZOS)
    im = ImageEnhance.Brightness(im).enhance(1.05)
    im = ImageEnhance.Color(im).enhance(1.03)
    return im.convert("RGBA")


def _brand(draw, x=42, y=32, dark=False):
    color = (25, 38, 58, 255) if dark else WHITE
    draw.text((x, y), "侨联地产", font=_font(28, True), fill=color,
              stroke_width=1 if not dark else 0, stroke_fill=(0, 0, 0, 90))
    draw.text((x, y + 34), "金边华人租房", font=_font(12, False), fill=color)


def minimal(src, out, project, layout, price):
    """方案A：实拍优先，仅左上角信息卡 + 右下价格。"""
    im = _base(src)
    panel = Image.new("RGBA", im.size, (0, 0, 0, 0)); d = ImageDraw.Draw(panel)
    d.rounded_rectangle((26, 22, 495, 235), 24, fill=(12, 25, 47, 192), outline=(255,255,255,55), width=1)
    im = Image.alpha_composite(im, panel); d = ImageDraw.Draw(im)
    _brand(d, 50, 42)
    d.text((50, 112), project, font=_fit(d, project, 52, 38, 400), fill=WHITE)
    d.rounded_rectangle((50, 174, 50 + 190, 222), 22, fill=(247,249,253,245))
    d.text((68, 180), layout, font=_font(26, True), fill=(25,76,165,255))
    price_box(d, price, white=False)
    im.convert("RGB").save(out, quality=95)


def editorial(src, out, project, layout, price):
    """方案B：杂志感底部白栏，没有暗罩。"""
    im = _base(src)
    d = ImageDraw.Draw(im)
    d.rounded_rectangle((30, 28, 220, 96), 16, fill=(255,255,255,232))
    _brand(d, 49, 35, dark=True)
    d.rectangle((0, 548, W, H), fill=(250,249,246,248))
    d.text((44, 574), project, font=_fit(d, project, 48, 38, 540), fill=(27,32,40,255))
    d.text((46, 642), f"{layout}  ·  侨联实拍", font=_font(25, True), fill=(63,72,85,255))
    d.rounded_rectangle((930, 580, 1242, 683), 18, fill=(24,81,181,255))
    d.text((958, 591), "租金", font=_font(15, False), fill=(209,226,255,255))
    d.text((958, 615), price, font=_font(43, True), fill=WHITE)
    im.convert("RGB").save(out, quality=95)


def premium(src, out, project, layout, price):
    """方案C：右侧实拍，左侧深蓝窄栏，更像专业地产画册。"""
    im = _base(src)
    panel = Image.new("RGBA", im.size, (0,0,0,0)); d = ImageDraw.Draw(panel)
    d.rectangle((0,0,430,H), fill=(10,31,65,230))
    for x in range(430, 560):
        d.line((x,0,x,H), fill=(10,31,65,int(230*(1-(x-430)/130))))
    im = Image.alpha_composite(im,panel); d=ImageDraw.Draw(im)
    _brand(d, 42, 38)
    d.text((42, 176), project, font=_fit(d, project, 54, 38, 350), fill=WHITE)
    d.rounded_rectangle((42,252,260,312), 28, fill=(245,248,254,250))
    d.text((66,261), layout, font=_font(28,True), fill=(27,75,163,255))
    d.text((42,370), "实拍房源", font=_font(22,True), fill=(219,229,245,255))
    d.text((42,410), "中文顾问 · 视频看房", font=_font(20,False), fill=(191,207,231,255))
    d.text((42,570), "租金", font=_font(17,False), fill=(169,191,225,255))
    d.text((42,600), price, font=_font(52,True), fill=WHITE)
    im.convert("RGB").save(out, quality=95)


def price_box(d, price, white=False):
    fill = (255,255,255,245) if white else BLUE
    fg = (21,83,190,255) if white else WHITE
    d.rounded_rectangle((974, 590, 1244, 686), 19, fill=fill, outline=(190,211,248,180), width=2)
    d.text((998, 600), "租金", font=_font(15,False), fill=fg)
    d.text((998, 623), price, font=_font(42,True), fill=fg)


def main():
    p=argparse.ArgumentParser(); p.add_argument("src"); p.add_argument("outdir")
    p.add_argument("--project",default="炳发城");p.add_argument("--layout",default="4房5卫1厅");p.add_argument("--price",default="$850/月")
    a=p.parse_args(); out=Path(a.outdir);out.mkdir(parents=True,exist_ok=True)
    minimal(a.src,out/"A_minimal.jpg",a.project,a.layout,a.price)
    editorial(a.src,out/"B_editorial.jpg",a.project,a.layout,a.price)
    premium(a.src,out/"C_premium.jpg",a.project,a.layout,a.price)


if __name__ == "__main__": main()
