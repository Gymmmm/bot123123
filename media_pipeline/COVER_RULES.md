# Cover selection rules (locked) — `cover_selector_v1`

Module: `cover_selector.py`  
API: `select_cover(paths) -> (best_path, scores_dict)`

## Prefer (bonus)

| Signal | Effect |
|--------|--------|
| Living-room–like interior | Strong bonus (`room_bonus` ≈ +0.22…+0.32) |
| Exterior / façade / outdoor | Strong bonus |
| Bedroom with good light | Medium bonus |
| High sharpness (Laplacian var) | From base quality + extra sharpness term |
| Landscape-ish (`w/h ≥ 1.05`, best ≥ 1.25) | `orient_bonus` +0.12…+0.18 |
| Good brightness / contrast / resolution | Via `photo_ranker.analyze_photo` quality score |

## Downrank / soft-reject

| Signal | Effect |
|--------|--------|
| Toilet / bathroom-dominant | Large negative bonus; soft-reject if toilet score > 0.6 |
| Kitchen-only | Mild negative bonus (kept if no better living/exterior) |
| Heavy bottom/corner text / watermark / contacts | `text_penalty`; soft-reject if `text_heavy ≥ 0.55` |
| Dark / low brightness | Base reject + cover downrank |
| Extreme portrait (`w/h < 0.62`) | Reject reason + score penalty |
| Duplicate-ish frames (MD5 / pHash) | Dropped by `deduplicate`; first/higher-scoring kept |

## Hard reject

- Unreadable / OpenCV read failure
- Too blurry (sharpness < ~35–40)
- Too small (&lt; 500px side) from base ranker

## Selection order

1. Analyze every path → quality + room heuristic + text-band score → `cover_score`
2. Deduplicate (exact MD5 + perceptual hash)
3. Drop hard rejects
4. Prefer pool without soft-rejects **and** label ∈ {living, exterior, bedroom}
5. Else any non soft-reject; else best remaining
6. Highest `cover_score` wins

## Score sketch

```
cover_score ≈
    quality * 0.55
  + sharpness_norm * 0.15
  + room_bonus
  + orient_bonus
  − text_penalty
  − extreme_portrait_penalty
```

## Limitations (v1)

- Room labels are **color/structure heuristics**, not a trained classifier — toilets and kitchens can be mis-tagged; living vs bedroom often share the same pool.
- Text/watermark detection is edge/glyph-blob based (no OCR) — styled logos and light watermarks may be missed; busy floors/gates can false-positive.
- No semantic “hero furniture” detection.

## Demo

```bash
.venv/bin/python cover_selector.py houses/7028 \
  -o output/media_pipeline_brand_v2/cover_pick
```

Writes `cover_pick_report.json` and copies the winner to `cover_winner.jpg`.
