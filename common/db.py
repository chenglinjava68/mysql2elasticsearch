#coding:utf8
# Author: zh3linux(zh3linux@gmail.com)

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from lib.sqlpool  import  SqlPool
from config import config

class DB():
    def __init__(self):
        self.conn = {}
        for db in config.mysql:
            MYSQLSETTING = config.mysql.__getattr__(db)
            self.conn[db] = self.NewDB(**MYSQLSETTING)

    def NewDB(self, **kwargs):
        pool = SqlPool(**kwargs)
        return pool

    def get_minid_and_maxid(self, conn, table, field):
        min_res = conn.query("select min(%s) as minid from %s" %(field, table))
        minid = 0
        if min_res:
            minid = min_res[0].get('minid', 0)
        max_res = conn.query("select max(%s) as maxid from %s" %(field, table))
        maxid = 0
        if max_res:
            maxid = max_res[0].get('maxid', 0)
        minid, maxid = int(minid), int(maxid)
        return minid, maxid

db = DB()
