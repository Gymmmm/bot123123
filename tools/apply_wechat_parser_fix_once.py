from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def replace_once(path: str, old: str, new: str) -> None:
    p = ROOT / path
    text = p.read_text(encoding="utf-8")
    if old not in text:
        raise SystemExit(f"expected block not found in {path}: {old[:80]!r}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# 1) Strip Unicode formatting/private-use noise at the shared source sanitizer.
replace_once(
    "source_sanitizer.py",
    "import re\nfrom dataclasses import dataclass\n",
    "import re\nimport unicodedata\nfrom dataclasses import dataclass\n",
)
replace_once(
    "source_sanitizer.py",
    "\n\n@dataclass(frozen=True)\nclass SanitizedSourceText:",
    '''\n\ndef strip_unicode_noise(value: str) -> str:\n    \"\"\"Remove source-app formatting/private-use code points without changing facts.\n\n    WeChat note exports can contain private-use glyph separators such as U+F003\n    between ordinary CJK characters. They are presentation noise, not source\n    evidence. Format controls (Cf) are treated the same way.\n    \"\"\"\n    return \"\".join(\n        ch for ch in str(value or \"\")\n        if unicodedata.category(ch) not in {\"Cf\", \"Co\"}\n    )\n\n\n@dataclass(frozen=True)\nclass SanitizedSourceText:''',
)
replace_once(
    "source_sanitizer.py",
    '    for original in str(raw_text or "").replace("\\r\\n", "\\n").split("\\n"):\n        line = original.strip()\n',
    '    cleaned_source = strip_unicode_noise(str(raw_text or "")).replace("\\r\\n", "\\n")\n    for original in cleaned_source.split("\\n"):\n        line = original.strip()\n',
)

# 2) Project metadata may declare a safe default property family. Explicit
# source property types remain authoritative; metadata is only a fallback.
replace_once(
    "qiaolian_dual/listing_taxonomy.py",
    '''class ProjectIdentity:\n    key: str\n    display: str\n    kind: str  # project or brand\n    aliases: tuple[str, ...]\n''',
    '''class ProjectIdentity:\n    key: str\n    display: str\n    kind: str  # project or brand\n    aliases: tuple[str, ...]\n    property_family: str | None = None\n''',
)
replace_once(
    "qiaolian_dual/listing_taxonomy.py",
    '    ProjectIdentity("the_pinnacle", "The Pinnacle 幸福广场", "project", ("the pinnacle", "太子幸福广场", "幸福广场", "prince happiness plaza")),\n',
    '    ProjectIdentity("the_pinnacle", "The Pinnacle 幸福广场", "project", ("the pinnacle", "太子幸福广场", "幸福广场", "prince happiness plaza"), property_family="公寓"),\n',
)
replace_once(
    "qiaolian_dual/listing_taxonomy.py",
    '''    family, subtype, property_display, property_status, property_evidence, property_flags = _extract_property(text)\n    project_alias_evidence = ([_evidence(project_alias, "raw_project_alias", "high", project_alias)] if project_alias else [])\n''',
    '''    family, subtype, property_display, property_status, property_evidence, property_flags = _extract_property(text)\n    if family == "未知" and project_key:\n        project_meta = next((item for item in PROJECT_IDENTITIES if item.key == project_key), None)\n        if project_meta and project_meta.kind == "project" and project_meta.property_family:\n            family = project_meta.property_family\n            subtype = None\n            property_display = project_meta.property_family\n            property_status = "inferred"\n            property_evidence = [\n                _evidence(\n                    project_meta.property_family,\n                    "project_property_metadata",\n                    "high",\n                    project_name or project_key,\n                )\n            ]\n            property_flags = [flag for flag in property_flags if flag != "unknown_property_type"]\n    project_alias_evidence = ([_evidence(project_alias, "raw_project_alias", "high", project_alias)] if project_alias else [])\n''',
)

# 3) Keep deal intent aligned with the rent extractor: "房间价格" is already an
# explicit monthly-rent label there. A bare USD amount is still not rent intent.
replace_once(
    "qiaolian_dual/canonical_facts.py",
    '        r"出租|招租|租金|月租|租赁|仅租|只租|for rent|only for rent|rent(?:al)? only|per month|/month|每月|/月",\n',
    '        r"出租|招租|租金|月租|租赁|房间价格|仅租|只租|for rent|only for rent|rent(?:al)? only|per month|/month|每月|/月",\n',
)

# 4) Regression coverage for the production WeChat-note shape.
test_path = ROOT / "tests/test_wechat_canonical_regressions.py"
test_path.write_text(r'''from source_sanitizer import sanitize_source_text, strip_unicode_noise\nfrom qiaolian_dual.canonical_facts import canonicalize_source\n\n\ndef _facts(text: str):\n    sanitized = sanitize_source_text(text).text\n    return canonicalize_source(\n        text,\n        sanitized_text=sanitized,\n        source_identity={"source_type": "wechat_note", "source_post_id": 1},\n        media_summary={"image_count": 8, "media_type": "image"},\n    )\n\n\ndef _pinnacle_text() -> str:\n    return (\n        "The Pinnacle 幸福广场\\n"\n        "位置：太子幸福广场\\n"\n        "户型：2房2厅2卫\\n"\n        "房间价格：850美金\\n"\n        "押1付1\\n"\n        "合同情况：1年\\n"\n        "楼层：39\\n"\n    )\n\n\ndef test_pinnacle_project_metadata_and_rental_semantics_are_publishable():\n    facts = _facts(_pinnacle_text())\n    assert facts["project_key"] == "the_pinnacle"\n    assert facts["project_name"] == "The Pinnacle 幸福广场"\n    assert facts["property_type"] == "公寓"\n    assert facts["property_type_status"] == "inferred"\n    assert facts["deal_type"] == "rent"\n    assert facts["monthly_rent_usd"] == 850\n    assert facts["deposit_payment_terms"] == "押1付1"\n    assert facts["contract_term_months"] == 12\n    assert "unknown_property_type" not in facts["quality"]["blocking_flags"]\n    assert "missing_rental_intent" not in facts["quality"]["blocking_flags"]\n\n\ndef test_wechat_private_use_format_noise_does_not_change_canonical_facts():\n    clean = _pinnacle_text()\n    noisy = clean.replace("幸福广场", "幸\\uf003福\\uf003广\\uf003场\\uf003")\n    noisy = noisy.replace("2房2厅2卫", "2房\\uf0032厅\\uf0032卫\\uf003")\n    noisy = noisy.replace("押1付1", "押\\uf0031付\\uf0031")\n    assert ord("\\uf003") == 0xF003\n    assert "\\uf003" not in strip_unicode_noise(noisy)\n    clean_facts = _facts(clean)\n    noisy_facts = _facts(noisy)\n    for key in (\n        "project_key", "project_name", "public_location_display", "layout",\n        "monthly_rent_usd", "deposit_payment_terms", "contract_term_months",\n        "property_type", "deal_type",\n    ):\n        assert noisy_facts[key] == clean_facts[key]\n    assert "\\uf003" not in noisy_facts["display_title"]\n\n\ndef test_bare_amount_without_rent_or_sale_semantics_stays_unknown():\n    facts = _facts(\n        "The Pinnacle 幸福广场\\n"\n        "户型：2房2厅2卫\\n"\n        "850美金\\n"\n    )\n    assert facts["deal_type"] == "unknown"\n    assert "missing_rental_intent" in facts["quality"]["blocking_flags"]\n\n\ndef test_unknown_project_without_property_evidence_stays_unknown_property_type():\n    facts = _facts(\n        "项目：Random Residence\\n"\n        "位置：BKK1\\n"\n        "户型：2房2卫\\n"\n        "租金：850美金/月\\n"\n    )\n    assert facts["deal_type"] == "rent"\n    assert facts["property_type"] == "未知"\n    assert facts["property_type_status"] == "unknown"\n    assert "unknown_property_type" in facts["quality"]["blocking_flags"]\n'''.replace('\\n', '\n'), encoding="utf-8")

# Run focused coverage before committing.
subprocess.run([
    "python", "-m", "pytest", "-q",
    "tests/test_wechat_canonical_regressions.py",
    "tests/test_parser_v12_regressions.py",
    "tests/test_area_gate_single_source.py",
], cwd=ROOT, check=True)

# Remove the one-shot automation from the resulting source commit.
(ROOT / ".github/workflows/apply-wechat-parser-fix-once.yml").unlink(missing_ok=True)
Path(__file__).unlink(missing_ok=True)

subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=ROOT, check=True)
subprocess.run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], cwd=ROOT, check=True)
subprocess.run(["git", "add", "-A"], cwd=ROOT, check=True)
subprocess.run(["git", "commit", "-m", "Fix WeChat canonical classification and Unicode noise"], cwd=ROOT, check=True)
subprocess.run(["git", "push", "origin", "HEAD:codex/qiaolian-ui-cleanup-20260829"], cwd=ROOT, check=True)
