from typing import Any
import json


def load_json(path: str) -> Any:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(e)
        return