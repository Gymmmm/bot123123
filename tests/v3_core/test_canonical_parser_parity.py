from itertools import product

from qiaolian_dual.canonical_facts import canonicalize_source as legacy_canonicalize_source
from v3_core.inventory.canonical_facts import canonicalize_source as v3_canonicalize_source


CASES = [
    """富力城 公寓\n2房1厅2卫\n面积95㎡ 19楼\n租金 $800/月\n押1付1 合同1年\n家具家电齐全 泳池 健身房""",
    """钻石岛公寓出售\n2房2卫\n面积88㎡\n售价 $120,000\n精装修""",
    """BKK1 公寓\n2房1厅\n租金 $680/月\n售价 $100,000\n押1付1\n面积90㎡""",
    """永旺1公寓 特价出租\n1房1厅\n原价 $900/月\n现价 $750/月\n押1付1\n租期1年\n65㎡""",
    """BKK1 apartment for rent\n2 bedrooms / 2 bathrooms\nUSD 950 per month\n85 sqm\nfloor 12""",
    """森速公寓出租\n2房1厅\n租金900美元/月\n物业费：已含\n网络：免费\n水费：$0.5/m3\n电费：$0.25/度\n停车费：$50/月""",
    """洪森大道别墅出租\n4房5卫\n土地尺寸 12m x 20m\n建筑尺寸 10m x 16m\n租金 $2500/月\n押2付1\n租期1年""",
    """水净华联排出租\n3房4卫\n面积：12.5 x 20\n租金 $1800/月\n押1付1""",
    """富力城 2房1厅 公寓出租\n月租 $800\n家具家电齐全\n每周2次保洁\n管家服务\n灭虫\n匹克球 乒乓球 儿童乐园""",
]


def _assert_same(raw: str, **kwargs):
    legacy = legacy_canonicalize_source(raw, **kwargs)
    v3 = v3_canonicalize_source(raw, **kwargs)
    assert v3 == legacy


def test_v3_canonical_parser_matches_locked_production_matrix():
    for raw in CASES:
        _assert_same(raw)


def test_v3_canonical_parser_matches_combinatorial_contract_space():
    projects = ("富力城", "BKK1", "钻石岛")
    layouts = ("1房1厅", "2房1厅2卫", "Studio")
    prices = ("租金 $500/月", "月租900美元/月", "现价 $1200/月 原价 $1500/月")
    terms = ("押1付1 租期1年", "押2付1 合同6个月", "")
    sizes = ("面积55㎡ 8楼", "面积95㎡ 19楼", "")
    for project, layout, price, term, size in product(projects, layouts, prices, terms, sizes):
        raw = "\n".join(value for value in (project, "公寓出租", layout, price, term, size) if value)
        _assert_same(raw)


def test_v3_canonical_parser_matches_sale_and_mixed_variants():
    for raw in (
        "富力城 2房1厅 出售 售价 $100,000 面积90㎡",
        "BKK1 1房1厅 租金$650/月 售价$88,000",
        "钻石岛 3房2卫 for sale sale price $250,000",
        "永旺1 2房1厅 for rent USD 900 per month",
    ):
        _assert_same(raw)


def test_v3_canonical_parser_matches_sanitized_and_identity_inputs():
    raw = "富力城 2房1厅 租金$800/月 联系微信 abc123"
    sanitized = "富力城 2房1厅 租金$800/月"
    kwargs = {
        "sanitized_text": sanitized,
        "source_identity": {"source_type": "telegram", "source_name": "test", "source_post_id": "123"},
        "media_summary": {"media_type": "image", "count": 5},
    }
    _assert_same(raw, **kwargs)


def test_v3_canonical_parser_matches_manual_overrides():
    raw = "2房1厅 租金$800/月"
    kwargs = {
        "manual_overrides": {
            "canonical_area_key": "bkk1",
            "canonical_area_display": "BKK1",
            "project_name": "测试项目",
        }
    }
    _assert_same(raw, **kwargs)
