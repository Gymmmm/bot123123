from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def test_parser_does_not_use_publication_copy_as_fact_input():
    text = (ROOT / 'ai_parser.py').read_text(encoding='utf-8')
    forbidden = [
        'publication_package',
        'channel_post',
        'caption_variant',
        'cover_generator',
        'review_note text is a fact input',
    ]
    for token in forbidden[:-1]:
        assert token not in text
    assert 'No template, caption, package or review-note text is a fact input.' in text
