import json
from pathlib import Path
from collections import OrderedDict


class ConfigParser:

    def __init__(self, args):
        args = args.parse_args()
        self.cfg_file_name = Path(args.config)
        self.config = read_json(self.cfg_file_name)

    def initialize(self, name, module, *args):
        module_name = self[name]['type']
        module_args = dict(self[name]['args'])
        return getattr(module, module_name)(*args, **module_args)

    def __getitem__(self, name):
        return self.config[name]


def read_json(file_name):
    file_name = Path(file_name)
    with file_name.open('rt') as handle:
        return json.load(handle, object_hook=OrderedDict)
