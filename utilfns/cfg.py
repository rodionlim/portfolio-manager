#!/usr/bin/env python3

import os
import json

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")


def read_cfg():
    with open(os.path.join(BASE_DIR, "config.json")) as f:
        cfg = json.load(f)
    return cfg