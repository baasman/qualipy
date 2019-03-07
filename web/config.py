import os
import json


HOME = os.path.expanduser('~')


class Config(object):

    def __init__(self):
        with open(os.path.join(HOME, '.qualipy', 'config.py'), 'r') as f:
            _config = json.load(f)
        for k, v in _config.items():
            setattr(self, k.upper(), v)
        self.REDIS_PORT = 6379
