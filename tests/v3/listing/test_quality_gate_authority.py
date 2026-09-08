from __future__ import annotations

import inspect

import qiaolian_v3.listing.dedupe as dedupe
import qiaolian_v3.listing.update_policy as update_policy
import qiaolian_v3.media.gallery as gallery
import qiaolian_v3.parser.quality_gate as quality_gate


def test_quality_gate_is_only_phase4_routing_decision_writer():
    assert 'routing_decision' in inspect.getsource(quality_gate)
    assert 'routing_decision' not in inspect.getsource(gallery)
    assert 'routing_decision' not in inspect.getsource(dedupe)
    assert 'routing_decision' not in inspect.getsource(update_policy)

# Phase 4 CI synchronization marker; no runtime behavior.
