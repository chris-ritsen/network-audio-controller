import json
from json import JSONEncoder
from typing import Any

class ToJsonEncoder(JSONEncoder):
    def default(self, obj):
        return getattr(obj.__class__, "to_json", super().default)(obj)

def dump_json_formatted(obj: Any) -> str:
    return json.dumps(obj, indent=2, cls=ToJsonEncoder)

def load_json_formatted(json_string: str) -> Any:
    return json.loads(json_string)