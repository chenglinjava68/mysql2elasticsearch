#coding:utf8
# Author: zh3linux(zenghuashan)

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from common import db

class DBTable():
    def __init__(self, dbconn, table, field, field_str):
        self.dbconn = dbconn
        self.table = table
        self.field = field
        self.field_str = field_str

    def get_min_max(self):
        minid, maxid = db.get_minid_and_maxid(self.dbconn, self.table, self.field)
        return minid, maxid

    def range_table(self, currentid):
        data_list = self.dbconn.query("select %s from %s where %s>=%d and %s<%d" %(self.field_str, self.table, self.field, currentid, self.field, currentid+1000))
        return data_list
