#coding:utf8
# Author: zh3linux(zenghuashan)

'''
# 全量数据导入 --> qing_group 表 和 qing_customer_group

直接通过for循环表的min(fileid)-->max(fileid)，
从最小fileid到某时刻最大fileid，一个个从MySQL中读出来，
组成ES-Docs并写入ES。
'''
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from common import db, es, dbtable
import logging, json
import traceback
from config import config

class TableDump():
    def __init__(self, tablename):
        table_setting = config.tables.get(tablename)
        if not table_setting:
            print 'args must in:', config.tables.keys()
            sys.exit()
        self.table = tablename
        self.db = db.conn[table_setting['db']]
        self.index = table_setting['index']
        self.esid = table_setting['esid']
        self.field = table_setting['field']
        self.selectstr = table_setting['selectstr']

        print self.index, self.table
#        print es.es.indices.put_mapping(index=self.index, doc_type=self.table, ignore=[400], body={"numeric_detection": True})
        self.dbtable = dbtable.DBTable(self.db, self.table, self.field, self.selectstr)

    def for_table(self):
        minid, maxid = self.dbtable.get_min_max()

        currentid = minid
        while (currentid < maxid):
            data_list = self.dbtable.range_table(currentid)
            currentid += 1000
            if not data_list:
                continue
            print currentid, len(data_list)
            es.bulk(data_list, index=self.index, doc_type=self.table, esid=self.esid)

    def run(self):
        self.for_table()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        logging.error("no input table num")
        sys.exit()
    table = sys.argv[1]
    TableDump(table).run()
