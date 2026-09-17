#!/usr/bin/env python3
"""V3 Publisher production entrypoint."""
from v3_core.publishing.publisher_product_policy import apply_publisher_product_policy
from v3_core.publishing.publisher_app import run


if __name__ == "__main__":
    apply_publisher_product_policy()
    run()
