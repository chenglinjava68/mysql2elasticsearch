#coding:utf8
# Author: zh3linux(zh3linux@gmail.com)

import yaml, os
from attrdict import AttrDict
CONFIGFILE = '/etc/esdump.conf'
rootPath = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

if not os.path.isfile(CONFIGFILE):
    CONFIGFILE = os.path.join(rootPath, 'config', 'esdump.conf')

with open(CONFIGFILE) as f:
    CONFIGYAML = yaml.load(f)

class Config():
     
    def __init__(self):
        pass

    def __getattr__(self, name):
        if isinstance(CONFIGYAML.get(name, {}), dict):
            return AttrDict(CONFIGYAML.get(name, {}))
        return CONFIGYAML.get(name, {})

    def get(self, key):
        pass

config = Config()

if __name__ == '__main__':
    print config.qing
