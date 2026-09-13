# Listing media pipeline v1

去源图水印/联系方式/旧 logo → 自动选主图 → 其余组图叠顶线金标 → 封面渲染。

## Run

```bash
python run_listing_media_pipeline.py \
  --input-dir /path/to/media \
  --output-dir ./output/pipeline_v1 \
  [--only 7028 7237] [--cover-limit 5]
```

Brand mark: `assets/qiaolian_logo_mark_gold.png` + full-width gold topline (`gallery_topline_brand.py`).
Enhance / auto-P: OFF by default.
