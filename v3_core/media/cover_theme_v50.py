"""Qiaolian v5.0 real-photo Pillow cover engine.

No photo generation, no Telegram messages, no inventory writes.
Keep the production CoverRenderData interface unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Any
from PIL import Image, ImageOps, ImageDraw, ImageFont, ImageFilter

ROOT = Path(__file__).resolve().parents[2]
PALETTES = {
    "minimal_white": ("#FFFFFF", "#242c34", "#A17B4B"),
    "business_blue": ("#E9F4FD", "#113A67", "#275F99"),
    "cream_beige": ("#FAF3EA", "#62492F", "#997347"),
    "nature_green": ("#F1F8EE", "#33583B", "#467B51"),
}
ALIASES = {"white": "minimal_white", "blue": "business_blue",
           "cream": "cream_beige", "green": "nature_green"}
KINDS_VILLA = ("别墅", "villa", "排屋", "townhouse", "独栋", "联排", "双拼")

@dataclass
class CoverSettings:
    width: int = 1200
    height: int = 1500
    theme: str = "cream_beige"
    layout: str = "auto"
    villa_hero_3: float = .63
    villa_hero_4: float = .60
    apartment_hero: float = .65
    footer_ratio: float = .105
    gap: int = 6
    max_photos: int = 4
    logo_path: str = ""
    font_regular: str = ""
    font_bold: str = ""

def _font(size, bold, settings):
    candidates = [
        settings.font_bold if bold else settings.font_regular,
        str(ROOT / "assets/fonts" / ("NotoSansSC-Bold.otf" if bold else "NotoSansSC-Regular.otf")),
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/System/Library/Fonts/PingFang.ttc",
    ]
    for p in candidates:
        if p:
            try: return ImageFont.truetype(str(p), size)
            except (OSError, ValueError): pass
    return ImageFont.load_default()

def _kind(data):
    value = str(getattr(data, "property_type", "") or "").lower()
    return "villa" if any(x in value for x in KINDS_VILLA) else "apartment"

def _layout(data, settings, count):
    if count == 1: return "single"
    if settings.layout in ("villa", "apartment"): return settings.layout
    return _kind(data)

def _regions(kind, count, w, h, gap, st):
    count = min(count,4)
    if count == 1:return [(0,0,w,h)]
    if kind == "villa":
        frac=st.villa_hero_3 if count<=3 else st.villa_hero_4
        left=int(w*frac)
        sh=(h-(count-2)*gap)//(count-1)
        boxes=[(0,0,left,h)]
        y=0
        for i in range(count-1):
            bh=sh if i<count-2 else h-y
            boxes.append((left+gap,y,w-left-gap,bh))
            y+=bh+gap
        return boxes
    top=int(h*st.apartment_hero)
    remaining=h-top-gap
    sw=(w-(count-2)*gap)//(count-1)
    boxes=[(0,0,w,top)]
    x=0
    for i in range(count-1):
        bw=sw if i<count-2 else w-x
        boxes.append((x,top+gap,bw,remaining))
        x+=bw+gap
    return boxes

def _fit(image, size, *, main=False, kind="apartment"):
    # Preserve more of exterior height; blurred sides use the SAME source photo.
    if main and kind=="villa" and image.width/image.height < size[0]/size[1]*.74:
        bg=ImageOps.fit(image,size,method=Image.Resampling.LANCZOS).filter(ImageFilter.GaussianBlur(26))
        fg=ImageOps.contain(image,size,method=Image.Resampling.LANCZOS)
        bg.paste(fg,((size[0]-fg.width)//2,(size[1]-fg.height)//2))
        return bg
    return ImageOps.fit(image,size,method=Image.Resampling.LANCZOS,
                        centering=(.5,.40 if main and kind=="villa" else .5))

def _compact(draw, value, max_width, fontsize, minimum, settings, bold=False):
    txt=" ".join(str(value or "").split())
    for n in range(fontsize,minimum-1,-2):
        f=_font(n,bold,settings)
        if draw.textbbox((0,0),txt,font=f)[2]<=max_width:return txt,f
    f=_font(minimum,bold,settings)
    while txt and draw.textbbox((0,0),txt+"…",font=f)[2]>max_width:
        txt=txt[:-1]
    return txt+("…" if txt else ""),f

def _logo(settings):
    path=Path(settings.logo_path) if settings.logo_path else ROOT/"media_pipeline/assets/qiaolian_logo_black_gold_lockup.png"
    if not path.is_file():return None
    with Image.open(path) as src:
        im=src.convert("RGBA")
    box=im.getchannel("A").getbbox()
    return im.crop(box) if box else None

def render_branded_cover(*, data: Any, images: Iterable[Image.Image],
                         output_path: str, settings: CoverSettings|None=None) -> str:
    st=settings or CoverSettings()
    if not (500<=st.width<=3500 and 500<=st.height<=3500):raise ValueError("invalid_cover_size")
    if st.gap < 0 or st.max_photos not in (1,2,3,4):raise ValueError("invalid_layout_settings")
    if not (.1<=st.footer_ratio<=.25):raise ValueError("invalid_footer_ratio")
    color=PALETTES[ALIASES.get(st.theme,st.theme)]
    pictures=[im.convert("RGB") for im in images][:st.max_photos]
    if not pictures:raise ValueError("cover_no_usable_images")
    kind=_layout(data,st,len(pictures))
    w,h=st.width,st.height
    footer_h=round(h*st.footer_ratio)
    area_h=h-footer_h
    canvas=Image.new("RGB",(w,h),color[0])
    boxes=_regions(kind,len(pictures),w,area_h,st.gap,st)
    for index,(im,(x,y,bw,bh)) in enumerate(zip(pictures,boxes)):
        if bw<=0 or bh<=0:raise ValueError("cover_layout_too_small")
        canvas.paste(_fit(im,(bw,bh),main=index==0,kind=kind),(x,y))
    d=ImageDraw.Draw(canvas)
    # Header-free image area + information-only footer, no promotional fillers.
    ftop=area_h
    d.line((0,ftop,w,ftop),fill=color[2],width=3)
    title=str(getattr(data,"project","") or getattr(data,"project_alias","") or getattr(data,"area","") or "").strip()
    area=str(getattr(data,"area","") or "").strip()
    typ=str(getattr(data,"property_type","") or "").strip()
    rooms=str(getattr(data,"layout","") or "").strip()
    summary=" · ".join(x for x in (typ,rooms) if x)
    price=data.price_line() if callable(getattr(data,"price_line",None)) else str(getattr(data,"price","") or "")
    # left 42% / center 29% / right 29%
    x1=int(w*.42);x2=int(w*.71)
    d.line((x1,ftop+26,x1,h-26),fill=color[2],width=2)
    d.line((x2,ftop+26,x2,h-26),fill=color[2],width=2)
    line1,ft=_compact(d,title or area, x1-68,47,25,st,True)
    d.text((28,ftop+25),line1,fill=color[1],font=ft)
    line2,sm=_compact(d,summary,x1-68,30,18,st)
    d.text((28,ftop+78),line2,fill=color[1],font=sm)
    d.text((x1+27,ftop+23),"月租金" if str(getattr(data,"deal_type","rent")).lower()=="rent" else "售价",
           fill=color[1],font=_font(22,False,st))
    price,fp=_compact(d,str(price),x2-x1-42,49,22,st,True)
    d.text((x1+24,ftop+57),price,fill=color[2],font=fp)
    logo=_logo(st)
    if logo:
        logo.thumbnail((w-x2-40,footer_h-50),Image.Resampling.LANCZOS)
        canvas.paste(logo,(x2+(w-x2-logo.width)//2,ftop+(footer_h-logo.height)//2),logo)
    else:
        label,lf=_compact(d,"侨联地产",w-x2-40,38,22,st,True)
        d.text((x2+22,ftop+49),label,font=lf,fill=color[1])
    dest=Path(output_path)
    dest.parent.mkdir(parents=True,exist_ok=True)
    if dest.suffix.lower() not in (".png",".jpg",".jpeg"):
        raise ValueError("output_must_be_jpg_or_png")
    if dest.suffix.lower()==".png":canvas.save(dest,optimize=True)
    else:canvas.save(dest,quality=93,optimize=True,subsampling=0)
    return str(dest)

# Labels from the existing media planner are authoritative when present.
# Do not infer an exterior from image dimensions: a portrait living room can
# otherwise incorrectly become the villa hero.
ROLE_WORDS = {
    "exterior": ("外观", "外景", "建筑", "门面", "房屋外", "exterior", "outside", "facade", "front view"),
    "living": ("客厅", "起居", "living", "lounge"),
    "bedroom": ("卧室", "主卧", "次卧", "bedroom"),
    "kitchen": ("厨房", "kitchen"),
    "dining": ("餐厅", "dining"),
    "bathroom": ("卫生间", "浴室", "bathroom"),
}

def detect_role(label: str, path: str = "") -> str:
    # Path keywords are low-confidence and only a fallback.
    for source in (str(label or "").lower(), Path(str(path)).stem.lower()):
        for role, tokens in ROLE_WORDS.items():
            if any(token in source for token in tokens):
                return role
    return "other"

def select_cover_roles(entries, kind, limit=4):
    """Stable, distinct room selection; unknowns preserve planner order.

    entries: [(path, role), ...].
    """
    priority = ("exterior", "living", "bedroom", "kitchen", "dining", "bathroom") if kind == "villa" else (
        "living", "bedroom", "kitchen", "dining", "bathroom", "exterior"
    )
    picked = []
    seen_paths = set()
    used_roles = set()
    for role in priority:
        for path, item_role in entries:
            if item_role != role or role in used_roles:
                continue
            key = str(Path(path).resolve())
            if key in seen_paths:
                continue
            picked.append(path)
            seen_paths.add(key)
            used_roles.add(role)
            break
        if len(picked) >= limit:
            return picked
    # When there are no reliable labels, prefer original curated order.
    for path, role in entries:
        key = str(Path(path).resolve())
        if key not in seen_paths:
            picked.append(path)
            seen_paths.add(key)
        if len(picked) >= limit:
            break
    return picked


def render_from_paths(*, data: Any, source_image: str, source_images=(),
                      source_labels=(), output_path: str,
                      settings: CoverSettings|None=None):
    """Keep production call style; select distinct curated roles when known."""
    st=settings or CoverSettings()
    paths = [source_image, *list(source_images or ())]
    labels = ["", *list(source_labels or ())]
    entries = []
    seen = set()
    for i, path in enumerate(paths):
        p = Path(str(path or ""))
        if not p.is_file():
            continue
        resolved = str(p.resolve())
        if resolved in seen:
            continue
        seen.add(resolved)
        label = labels[i] if i < len(labels) else ""
        entries.append((str(p), detect_role(label, str(p))))
    chosen = select_cover_roles(entries, _kind(data), st.max_photos)
    photos = []
    for path in chosen:
        try:
            with Image.open(path) as raw:
                photos.append(ImageOps.exif_transpose(raw).convert("RGB"))
        except (OSError, ValueError):
            continue
    return render_branded_cover(
        data=data, images=photos, output_path=output_path, settings=st
    )

def paginate_originals(paths, page_size=4):
    """Returns all genuine photo paths, never the composite cover."""
    if page_size!=4:raise ValueError("detail_page_size_must_be_4")
    valid=[];seen=set()
    for raw in paths:
        p=Path(str(raw or ""))
        if not p.is_file():continue
        key=str(p.resolve())
        if key not in seen:
            seen.add(key);valid.append(str(p))
    return [valid[i:i+4] for i in range(0,len(valid),4)]
