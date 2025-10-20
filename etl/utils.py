import json
from pathlib import Path

def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))

def save_json(path, obj):
    Path(path).write_text(json.dumps(obj, indent=2), encoding="utf-8")
