"""Install the locked channel caption builder onto publication_package."""
from __future__ import annotations


def install_locked_caption() -> None:
    try:
        import publication_package
        from qiaolian_dual.channel_post import format_button_post_text
    except Exception:
        return
    publication_package.format_button_post_text = format_button_post_text
