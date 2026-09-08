# Qiaolian Production Extraction

This branch is the cleaned production runtime extracted from the legacy repository.

## Runtime

All production code lives under `qiaolian_production/`.

Entrypoints:
- `qiaolian_production/run_collector.py`
- `qiaolian_production/run_publisher.py`
- `qiaolian_production/run_user.py`

Validation is handled by `.github/workflows/production-extraction-check.yml`.

The extracted runtime is required to import and build without falling back to legacy source files outside `qiaolian_production/`.

This branch is not the production deployment branch and does not deploy automatically.
