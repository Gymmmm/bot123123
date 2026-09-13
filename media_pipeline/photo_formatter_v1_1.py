from pathlib import Path
from PIL import Image, ImageOps
# =========================
# 基础配置
# =========================
JPEG_QUALITY = 92
BG_COLOR = (255, 255, 255)
# 原图与画布边缘的最小安全边距
PADDING = 12
# 横竖方判断
LANDSCAPE_THRESHOLD = 1.10
PORTRAIT_THRESHOLD = 0.90
# 三种完整实拍画布
CANVAS_PRESETS = {
    "landscape": {
        "size": (1200, 900),
        "logo_width_ratio": 0.20,
    },
    "portrait": {
        "size": (900, 1200),
        "logo_width_ratio": 0.25,
    },
    "square": {
        "size": (1080, 1080),
        "logo_width_ratio": 0.22,
    }
}
# Logo 配置
LOGO_MARGIN_X_RATIO = 0.035
LOGO_MARGIN_Y_RATIO = 0.025
LOGO_OPACITY = 0.96
# =========================
# 判断图片方向
# =========================
def detect_orientation(width, height):
    ratio = width / max(height, 1)
    if ratio >= LANDSCAPE_THRESHOLD:
        return "landscape"
    if ratio <= PORTRAIT_THRESHOLD:
        return "portrait"
    return "square"
# =========================
# Logo 处理
# =========================
def apply_logo_opacity(logo, opacity=LOGO_OPACITY):
    logo = logo.convert("RGBA")
    alpha = logo.getchannel("A")
    alpha = alpha.point(
        lambda p: int(p * opacity)
    )
    logo.putalpha(alpha)
    return logo
def resize_logo(logo, canvas_width, width_ratio):
    target_width = int(
        canvas_width * width_ratio
    )
    scale = target_width / logo.width
    target_height = max(
        1,
        int(logo.height * scale)
    )
    return logo.resize(
        (target_width, target_height),
        Image.Resampling.LANCZOS
    )
def paste_logo(
    canvas,
    logo,
    position="top_left",
    margin_x_ratio=LOGO_MARGIN_X_RATIO,
    margin_y_ratio=LOGO_MARGIN_Y_RATIO
):
    canvas_w, canvas_h = canvas.size
    logo_w, logo_h = logo.size
    margin_x = int(
        canvas_w * margin_x_ratio
    )
    margin_y = int(
        canvas_h * margin_y_ratio
    )
    if position == "top_right":
        x = canvas_w - logo_w - margin_x
        y = margin_y
    elif position == "bottom_left":
        x = margin_x
        y = canvas_h - logo_h - margin_y
    elif position == "bottom_right":
        x = canvas_w - logo_w - margin_x
        y = canvas_h - logo_h - margin_y
    else:
        # 默认左上
        x = margin_x
        y = margin_y
    overlay = canvas.convert("RGBA")
    overlay.alpha_composite(
        logo,
        (x, y)
    )
    return overlay.convert("RGB")
# =========================
# 原图完整适配画布
# =========================
def contain_image(
    src,
    canvas_size,
    padding=PADDING,
    bg_color=BG_COLOR
):
    canvas_w, canvas_h = canvas_size
    max_w = canvas_w - padding * 2
    max_h = canvas_h - padding * 2
    scale = min(
        max_w / src.width,
        max_h / src.height
    )
    new_w = max(
        1,
        int(src.width * scale)
    )
    new_h = max(
        1,
        int(src.height * scale)
    )
    resized = src.resize(
        (new_w, new_h),
        Image.Resampling.LANCZOS
    )
    canvas = Image.new(
        "RGB",
        canvas_size,
        bg_color
    )
    x = (canvas_w - new_w) // 2
    y = (canvas_h - new_h) // 2
    canvas.paste(
        resized,
        (x, y)
    )
    return canvas
# =========================
# 单张完整实拍
# =========================
def format_gallery_photo(
    input_path,
    output_path,
    logo_path=None,
    logo_position="top_left",
    quality=JPEG_QUALITY
):
    """
    完整实拍规则：
    横图:
        1200x900
        4:3
    竖图:
        900x1200
        3:4
    方图:
        1080x1080
        1:1
    原图：
        完整显示
        不裁切
        不拉伸
    Logo：
        默认左上
        白色透明 Logo
        保留来源原有水印
    """
    input_path = Path(input_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )
    with Image.open(input_path) as src:
        src = ImageOps.exif_transpose(src)
        src = src.convert("RGB")
        orientation = detect_orientation(
            src.width,
            src.height
        )
        preset = CANVAS_PRESETS[
            orientation
        ]
        canvas_size = preset[
            "size"
        ]
        canvas = contain_image(
            src,
            canvas_size
        )
        if logo_path:
            logo_path = Path(
                logo_path
            )
            if logo_path.exists():
                with Image.open(logo_path) as logo:
                    logo = logo.convert("RGBA")
                    logo = resize_logo(
                        logo,
                        canvas_width=canvas_size[0],
                        width_ratio=preset[
                            "logo_width_ratio"
                        ]
                    )
                    logo = apply_logo_opacity(
                        logo
                    )
                    canvas = paste_logo(
                        canvas,
                        logo,
                        position=logo_position
                    )
        canvas.save(
            output_path,
            "JPEG",
            quality=quality,
            optimize=True,
            progressive=True
        )
    return {
        "input": str(input_path),
        "output": str(output_path),
        "orientation": orientation,
        "canvas": {
            "width": canvas_size[0],
            "height": canvas_size[1]
        }
    }
# =========================
# 整套完整实拍
# =========================
def format_gallery_folder(
    input_folder,
    output_folder,
    logo_path=None,
    logo_position="top_left"
):
    """
    保持来源原始文件顺序。
    不按横竖分组。
    不按质量评分重排。
    """
    input_folder = Path(
        input_folder
    )
    output_folder = Path(
        output_folder
    )
    output_folder.mkdir(
        parents=True,
        exist_ok=True
    )
    exts = {
        ".jpg",
        ".jpeg",
        ".png",
        ".webp"
    }
    # 文件名若原本是 01.jpg / 02.jpg，
    # sorted 就能保持来源顺序
    files = sorted(
        [
            p
            for p in input_folder.iterdir()
            if p.suffix.lower() in exts
        ],
        key=lambda p: p.name
    )
    results = []
    for index, src in enumerate(
        files,
        start=1
    ):
        dst = (
            output_folder
            / f"{index:02d}.jpg"
        )
        info = format_gallery_photo(
            input_path=src,
            output_path=dst,
            logo_path=logo_path,
            logo_position=logo_position
        )
        info["order"] = index
        results.append(
            info
        )
    return results
# =========================
# CLI
# =========================
if __name__ == "__main__":
    import sys
    import json
    input_folder = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "houses/QC0089"
    )
    output_folder = (
        sys.argv[2]
        if len(sys.argv) > 2
        else "processed/QC0089/gallery"
    )
    logo_path = (
        sys.argv[3]
        if len(sys.argv) > 3
        else "assets/qiaolian_logo_white.png"
    )
    result = format_gallery_folder(
        input_folder=input_folder,
        output_folder=output_folder,
        logo_path=logo_path
    )
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2
        )
    )
