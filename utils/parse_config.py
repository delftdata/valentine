import json
from pathlib import Path
from collections import OrderedDict


class ConfigParser:
    """
    A class describing a configuration file parser

     Attributes
    ----------
    cfg_file_name : Path
        The file path of the configuration file
    config : OrderedDict
        The json configuration file's contents

    Methods
    -------
    initialize(name, module, *args)
        Function that initializes an object based of the configuration

    """

    def __init__(self, args):
        """
        Init function that reads a json configuration file
        Parameters
        ----------
        args
            Configuration file path
        """
        args = args.parse_args()
        self.cfg_file_name = Path(args.config)
        self.config = read_json(self.cfg_file_name)

    def initialize(self, name, module, *args):
        """
        Function that initializes an object based of the configuration

        Parameters
        ----------
        name
            The name of the object's class
        module
            The name of the module
        args
            The arguments of the init method of the object's class

        Returns
        -------
        The initialized object
        """
        module_name = self[name]['type']
        module_args = dict(self[name]['args'])
        return getattr(module, module_name)(*args, **module_args)

    def __getitem__(self, name):
        return self.config[name]


def read_json(file_name: str):
    """
    Function that reads a json file

    Parameters
    ----------
    file_name: str
        The json file path

    Returns
    -------
    OrderedDict
        The json file's contents

    """
    file_name = Path(file_name)
    with file_name.open('rt') as handle:
        return json.load(handle, object_hook=OrderedDict)
