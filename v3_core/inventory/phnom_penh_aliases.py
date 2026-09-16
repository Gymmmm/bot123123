"""Phnom Penh rental-market aliases used by the V3 listing taxonomy.

Keep this file market-facing: Chinese agent shorthand, common English project
names and renter-facing location names belong here. Developer/brand names stay
brands and must not silently become a specific project.
"""
from __future__ import annotations

from typing import Any


PROJECTS: tuple[tuple[str, str, tuple[str, ...], str | None], ...] = (
    ("j_tower_1", "J Tower 1", ("j tower 1", "j-tower 1", "jtower1", "jt1", "j tower一期"), "公寓"),
    ("j_tower_2", "J Tower 2", ("j tower 2", "j-tower 2", "jtower2", "jt2", "j2", "j tower二期"), "公寓"),
    ("j_tower_3", "J Tower 3", ("j tower 3", "j-tower 3", "jtower3", "jt3", "j3", "j tower三期"), "公寓"),
    ("le_conde_bkk1", "王府·观邸 Le Condé BKK1", ("le condé bkk1", "le conde bkk1", "le condé", "le conde", "leconde", "王府观邸", "王府·观邸", "乐康德"), "公寓"),
    ("picasso_city_garden", "Picasso City Garden 毕加索", ("picasso city garden", "picasso garden city", "picasso garden", "毕加索", "毕加索城市花园", "毕加索一期"), "公寓"),
    ("urban_village_1", "首都·国金 Urban Village", ("urban village phase 1", "urban village 1", "uv1", "urban village一期", "首都国金一期", "国金一期"), "公寓"),
    ("urban_village_2", "首都·国金 Urban Village Phase 2", ("urban village phase 2", "urban village 2", "uv2", "urban village二期", "首都国金二期", "国金二期"), "公寓"),
    ("the_peak", "The Peak 香格里拉", ("the peak", "peak", "the peak residence", "the peak residences", "the peak公寓", "the peak香格里拉", "香格里拉", "香格里拉公寓", "巅峰"), "公寓"),
    ("diamond_bay_garden", "钻石湾花园", ("diamond bay garden", "diamond bay gardens", "钻石湾", "钻石湾花园"), "公寓"),
    ("diamond_one", "钻石一号", ("diamond one", "diamond 1", "钻石一号", "钻石1号"), "公寓"),
    ("diamond_twin_tower", "钻石双子塔", ("diamond twin tower", "diamond twin towers", "diamond twin", "钻石双子塔", "钻石双塔"), "公寓"),
    ("mesong", "MESONG 金汇梅松", ("mesong", "mesong tower", "mesong towers", "金汇", "金汇梅松", "梅松"), "公寓"),
    ("rose_garden", "玫瑰滨江园", ("rose garden", "rose garden condominium", "rose garden condo", "rose garden riverside", "玫瑰滨江园", "玫瑰滨江", "玫瑰园", "滨江玫瑰园", "玫瑰园公寓"), "公寓"),
    ("prince_central_plaza", "Prince Central Plaza 太子中央广场", ("prince central plaza", "太子中央广场", "太子中央", "prince central"), "公寓"),
    ("prince_international_plaza", "太子国际广场", ("prince international plaza", "prince international", "太子国际广场", "太子国际", "国际广场"), "公寓"),
    ("prince_huan_yu_center", "太子·寰宇中心", ("prince huan yu center", "prince huanyu center", "prince huan yu", "huan yu center", "太子寰宇中心", "太子·寰宇中心", "太子寰宇", "寰宇中心", "寰宇"), "公寓"),
    ("prince_modern_plaza", "太子现代广场", ("prince modern plaza", "太子现代广场", "太子现代", "现代广场"), "公寓"),
    ("royal_platinum", "皇家铂金 Royal Platinum", ("royal platinum", "royal platinum condominium", "皇家铂金", "皇家白金", "皇家铂金公寓"), "公寓"),
    ("royal_park", "皇家花园公寓", ("royal park", "royal park condo", "royal park condominium", "皇家花园", "皇家花园公寓"), "公寓"),
    ("morgan_enmaison", "摩根天御 Morgan EnMaison", ("morgan enmaison", "morgan en maison", "enmaison", "摩根天御", "摩根天御公寓"), "公寓"),
    ("morgan_tower", "Morgan Tower 摩根大厦", ("morgan tower", "morgan tower phnom penh", "摩根大厦", "摩根塔"), "公寓"),
    ("peninsula_private_residence", "半岛御景", ("the peninsula", "peninsula private residence", "peninsula residence", "半岛御景", "半岛御景公寓"), "公寓"),
    ("wealth_mansion", "威尔斯公馆", ("wealth mansion", "wealth mansion phnom penh", "威尔斯", "威尔斯公馆"), "公寓"),
    ("orkide_royal", "ORKIDE皇家公寓", ("orkide royal", "orkide", "orkide royal condominium", "奥凯德", "奥凯德皇家", "奥凯德皇家公寓", "皇家公寓"), "公寓"),
    ("one_park", "金边壹号 ONE PARK", ("one park", "onepark", "one park residence", "金边壹号", "金边一号", "壹号"), "公寓"),
    ("olympia_city", "奥林匹亚城 Olympia City", ("olympia city", "the olympia city", "奥林匹亚城", "奥林匹克城"), "公寓"),
    ("time_square_1", "Time Square 1", ("time square 1", "timesquare 1", "ts1", "时代广场1", "时代广场一期"), "公寓"),
    ("time_square_2", "Time Square 2", ("time square 2", "timesquare 2", "ts2", "时代广场2", "时代广场二期"), "公寓"),
    ("time_square_3", "Time Square 3", ("time square 3", "timesquare 3", "ts3", "时代广场3"), "公寓"),
    ("time_square_5", "Time Square 5", ("time square 5", "timesquare 5", "ts5", "时代广场5"), "公寓"),
    ("time_square_7", "Time Square 7", ("time square 7", "timesquare 7", "ts7", "时代广场7"), "公寓"),
    ("time_square_8", "Time Square 8", ("time square 8", "timesquare 8", "ts8", "时代广场8"), "公寓"),
    ("time_square_9", "Time Square 9", ("time square 9", "timesquare 9", "ts9", "时代广场9", "the gatsby"), "公寓"),
    ("time_square_11", "Time Square 11", ("time square 11", "timesquare 11", "ts11", "时代广场11"), "公寓"),
    ("vue_aston", "Vue Aston 滨江雅诗顿", ("vue aston", "vueaston", "滨江雅诗顿", "滨江·雅诗顿", "御景阿斯顿", "阿斯顿"), "公寓"),
    ("agile_sky_residence", "Agile Sky Residence", ("agile sky residence", "agile sky", "雅居乐天悦"), "公寓"),
    ("chief_tower", "Chief Tower", ("chief tower", "chief tower phnom penh", "首峰大厦"), "公寓"),
    ("de_castle_royal", "De Castle Royal", ("de castle royal", "decastle royal", "de castle", "帝卡斯皇家"), "公寓"),
    ("m_residence", "M Residence", ("m residence", "m-residence", "m公寓"), "公寓"),
    ("the_view", "The View Serviced Residence", ("the view serviced residence", "the view residence", "the view bkk1", "the view"), "公寓"),
    ("sky_villa", "Sky Villa 天空别墅", ("sky villa", "skyvilla", "天空别墅"), "公寓"),
    ("odom_living", "Odom Living", ("odom living", "odom", "odom residence"), "公寓"),
    ("parc_21", "Parc 21 金柬·柏盛", ("parc 21", "parc21", "parc 21 residence", "金柬柏盛", "金柬·柏盛", "柏盛21"), "公寓"),
    ("la_vista_one", "紫晶壹号 La Vista One", ("la vista one", "lavista one", "紫晶壹号", "紫晶一号", "紫晶"), "公寓"),
    ("phnom_penh_galaxy_garden", "金边星河花园", ("phnom penh galaxy garden", "galaxy garden", "金边星河花园", "金边星河", "星河花园", "星河"), "公寓"),
    ("one_70", "ONE 70 金边首座", ("one 70", "one70", "one-70", "金边首座", "金边首座one70"), "公寓"),
    ("ding_li_tower", "Ding Li Tower 鼎立大厦", ("ding li tower", "dingli tower", "鼎立", "鼎立大厦"), "公寓"),
)

PROJECT_ALIAS_EXTENSIONS: dict[str, tuple[str, ...]] = {
    "the_bridge": ("bridge", "桥牌公寓", "世桥", "the bridge club"),
    "the_pinnacle": ("太子幸福", "太子幸福广场", "幸福广场", "太子幸福公寓", "prince happiness", "prince happiness plaza"),
    "rf_city": ("富力", "富力金边", "富力金边中心城", "金边中心城", "r&f", "r and f city"),
    "peng_huoth": ("炳发", "penghuoth"),
}

# These are common Chinese-market names but not stable enough to force into one
# canonical project without additional location/developer context.
CANDIDATE_PROJECT_ALIASES: dict[str, tuple[str, ...]] = {
    "威尼斯": ("威尼斯", "威尼斯公寓", "威尼斯城", "venice", "the venice", "venice residence"),
    "名士城": ("名士城", "名仕城", "名士公寓"),
    "幸福公寓": ("幸福公寓", "幸福国际", "幸福城"),
}

MARKET_LOCATIONS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("莫尼旺大道", "莫尼旺大道", "corridor", ("莫尼旺", "莫尼旺大道", "monivong", "monivong boulevard", "monivong blvd", "93路")),
    ("诺罗敦大道", "诺罗敦大道", "corridor", ("诺罗敦", "诺罗敦大道", "norodom", "norodom boulevard", "norodom blvd", "41路")),
    ("毛泽东大道", "毛泽东大道", "corridor", ("毛泽东大道", "毛泽东路", "mao tse toung", "mao tse toung blvd", "mao tse tung boulevard", "mao tse tung blvd", "245路")),
    ("俄罗斯大道", "俄罗斯大道", "corridor", ("俄罗斯大道", "russian boulevard", "russian federation boulevard", "russian federation blvd")),
    ("西哈努克大道", "西哈努克大道", "corridor", ("西哈努克大道", "西哈努克", "sihanouk boulevard", "sihanouk blvd")),
    ("6号路", "6号路附近", "corridor", ("6号路", "6a", "6a路", "nr6", "national road 6", "national road 6a")),
    ("271路", "271路附近", "corridor", ("271", "271路", "271公路", "st 271", "street 271")),
    ("2004路", "2004路附近", "corridor", ("2004", "2004路", "2004公路", "st 2004", "street 2004")),
    ("万谷湖", "万谷湖", "district", ("万谷湖", "boeung kak", "boeung kak 1", "boeung kak 2")),
    ("马卡拉", "马卡拉", "district", ("马卡拉", "玛卡拉", "7 makara", "prampir makara")),
    ("棉芷", "棉芷", "district", ("棉芷", "mean chey", "meanchey")),
    ("乌亚西市场", "乌亚西市场", "nearby", ("乌亚西", "乌鸦西", "orussey", "orussey market")),
    ("塔仔山", "塔仔山附近", "nearby", ("塔仔山", "塔山", "wat phnom")),
    ("独立碑", "独立碑附近", "nearby", ("独立碑", "独立纪念碑", "independence monument")),
    ("皇宫", "皇宫附近", "nearby", ("皇宫", "王宫", "大皇宫", "金边皇宫", "royal palace")),
    ("Naga", "金界附近", "nearby", ("naga", "naga world", "nagaworld", "naga 1", "naga 2", "金界", "金界附近")),
    ("新机场", "德崇机场方向", "nearby", ("新机场", "德崇机场", "techo international airport", "techo airport")),
    ("永旺3", "永旺3附近", "nearby", ("永旺3", "永旺三", "aeon3", "aeon 3", "aeon mall 3", "aeon mall3", "永旺3附近")),
    ("Boeung Snor", "Boeung Snor", "district", ("boeung snor", "boeung snor炳发", "炳发boeung snor")),
    ("Norea", "Norea附近", "nearby", ("norea", "koh norea", "norea附近", "norea炳发")),
    ("集茂271", "集茂271附近", "nearby", ("集茂271", "集茂271附近", "chip mong 271", "chipmong 271", "271集茂")),
)

MARKET_ALIAS_EXTENSIONS: dict[str, tuple[str, ...]] = {
    "BKK": ("西哈努克bkk",),
    "BKK1": ("boeung keng kang 1", "bkk一区"),
    "BKK2": ("boeung keng kang 2",),
    "BKK3": ("boeung keng kang 3",),
    "TK/7月区": ("tk区",),
    "百色河": ("tonle bassac", "tonle basak"),
    "森速": ("新金边", "新金边区"),
    "水净华": ("鸭子岛", "chroy changvar peninsula"),
    "桑园": ("chamkarmon", "chamkar mon"),
    "隆边": ("daun penh"),
    "铁桥头": ("chbar ampov",),
    "俄罗斯市场": ("俄市", "ttp", "tuol tom poung", "toul tom poung"),
    "中央市场": ("新街市", "central market", "phsar thmei"),
    "奥林匹克": ("olympic stadium", "olympia city"),
    "钻石岛": ("diamond island", "koh pich", "钻岛"),
    "河边": ("sisowath quay",),
    "金街": ("金街旁", "the bridge一带", "bridge附近"),
    "富力城": ("富力金边中心城", "r and f city"),
    "洪森大道": ("60米", "60米大道", "60米路", "hun sen blvd", "hun sen boulevard", "samdech hun sen blvd", "samdech hun sen boulevard", "ph60m"),
    "一号路": ("一号路炳发", "1号路炳发", "一号公路", "1号公路", "national road 1", "nr1"),
    "598路": ("street 598", "st 598"),
    "50米路": ("50m road", "50m boulevard"),
    "永旺商圈": ("aeon mall 1", "aeon mall1", "永旺1附近"),
    "永旺2": ("aeon mall 2", "aeon mall2"),
    "铁桥头": ("铁桥头炳发", "chbar ampov"),
}


def apply_phnom_penh_aliases(taxonomy: Any) -> None:
    """Add market aliases once to the existing V3 taxonomy module."""
    project_keys = {item.key for item in taxonomy.PROJECT_IDENTITIES}
    projects = list(taxonomy.PROJECT_IDENTITIES)
    for key, display, aliases, family in PROJECTS:
        if key not in project_keys:
            projects.append(taxonomy.ProjectIdentity(key, display, "project", aliases, property_family=family))
    extended_projects = []
    for item in projects:
        extra = PROJECT_ALIAS_EXTENSIONS.get(item.key, ())
        aliases = tuple(dict.fromkeys((*item.aliases, *extra)))
        extended_projects.append(taxonomy.ProjectIdentity(item.key, item.display, item.kind, aliases, item.property_family))
    taxonomy.PROJECT_IDENTITIES = tuple(extended_projects)

    market_keys = {item.key for item in taxonomy.MARKET_LOCATIONS}
    markets = list(taxonomy.MARKET_LOCATIONS)
    for key, display, relation, aliases in MARKET_LOCATIONS:
        if key not in market_keys:
            markets.append(taxonomy.MarketLocation(key, display, relation, aliases))
    extended_markets = []
    for item in markets:
        extra = MARKET_ALIAS_EXTENSIONS.get(item.key, ())
        aliases = tuple(dict.fromkeys((*item.aliases, *extra)))
        extended_markets.append(taxonomy.MarketLocation(item.key, item.display, item.relation, aliases))
    taxonomy.MARKET_LOCATIONS = tuple(extended_markets)
