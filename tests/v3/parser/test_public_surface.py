from __future__ import annotations

import qiaolian_v3.parser.canonical as canonical


def test_canonical_public_surface_excludes_legacy_projection_and_gate_helpers():
    legacy_names = {
        'draft_projection',
        'is_buildable',
        'has_confirmed_physical_area',
    }

    assert legacy_names.isdisjoint(set(canonical.__all__))
    for name in legacy_names:
        assert not hasattr(canonical, name)
