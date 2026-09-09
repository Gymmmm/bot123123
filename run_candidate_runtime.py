#!/usr/bin/env python3
from __future__ import annotations

import argparse

from candidate_runtime.launch import run_component


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one isolated V3 candidate component.")
    parser.add_argument("component", choices=("collector", "worker", "publisher", "user"))
    args = parser.parse_args()
    run_component(args.component)


if __name__ == "__main__":
    main()
