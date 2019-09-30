import os
import json


HOME = os.path.expanduser("~")


class Config(object):
    def __init__(self):
        try:
            with open(os.path.join(HOME, ".qualipy", "config.json"), "r") as f:
                _config = json.load(f)
        except FileNotFoundError:
            with open(os.path.join(HOME, ".qualipy", "config.json"), "w") as f:
                json.dump({}, f)
            _config = {}
        for k, v in _config.items():
            setattr(self, k.upper(), v)
