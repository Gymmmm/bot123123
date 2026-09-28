from pathlib import Path
import sys
sys.path.insert(0, str(Path("/Users/a1/projects/bot123123")))
from v3_core.media.cover_generator import CoverRenderData, generate_cover

PHOTO = Path("/Users/a1/.cursor/projects/Users-a1-projects-bot123123/assets/B22E5C52-2120-402B-8FFD-EEBC15A1E529-078a2c14-be71-4de3-b508-74820b48b6d9.jpg")
OUT = Path("/Users/a1/projects/bot123123/tmp_premium_show.jpg")

data = CoverRenderData(
    public_listing_id="L001",
    project="一号路炳发城",
    layout="4房1厅",
    area="BKK1",
    floor="1/3楼",
    price="1450",
    deal_type="rent",
    highlight_1="带车位",
    highlight_2="拎包入住",
    highlight_3="近学校",
)
out = generate_cover(
    style="premium_photo",
    source_image=str(PHOTO),
    output_path=str(OUT),
    data=data,
)
print(out)
