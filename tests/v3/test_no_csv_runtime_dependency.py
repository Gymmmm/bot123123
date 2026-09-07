from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_parser_and_publication_core_do_not_read_houses_csv():
    paths = [
        ROOT / 'ai_parser.py',
        ROOT / 'publication_package.py',
        ROOT / 'publication_delivery.py',
        ROOT / 'qiaolian_dual' / 'canonical_fact_projection.py',
        ROOT / 'qiaolian_dual' / 'publishability_contract.py',
    ]
    for path in paths:
        text = path.read_text(encoding='utf-8')
        assert 'houses.csv' not in text
        assert 'csv.DictReader' not in text
