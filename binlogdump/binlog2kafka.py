#coding:utf8
# Author: zh3linux(zh3linux@gmail.com)
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import logging
import json
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import *
from pymysqlreplication.event import *

from datetime import datetime
from common import kfk

from config import config

class SyncBinlog():
    def __init__(self, dbname, log_file=None, log_pos=None):
        self.log_file = log_file
        self.log_pos = log_pos
        self.mysql_settings =config.repl.get(dbname)
        self.current_binlog = None
        if not self.mysql_settings:
            print "not db, please input in", config.repl.keys()
            sys.exit()

    def for_binlog(self):
        stream = BinLogStreamReader(connection_settings=self.mysql_settings,
                                    server_id=1,log_pos=self.log_pos,log_file=self.log_file,
                                    blocking=True,resume_stream = True,
                                    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, RotateEvent])


        for binlogevent in stream:
            if isinstance(binlogevent, RotateEvent):
                self.current_binlog =  binlogevent.next_binlog
                continue

            for row in binlogevent.rows:
                if isinstance(binlogevent, DeleteRowsEvent):
                    row['db_rows_event'] = 'DELETE'
                elif isinstance(binlogevent, UpdateRowsEvent):
                    row['db_rows_event'] = 'UPDATE'
                    row['values'] = row.pop('after_values')
                    row['before_values'] = self.nil2null(row['before_values'])
                elif isinstance(binlogevent, WriteRowsEvent):
                    row['db_rows_event'] = 'WRITE'

                row['db_table_name'] = binlogevent.table
                try:
                    table_split = binlogevent.table.split('_')
                    int(table_split[-1])
                    row['db_type'] = '_'.join(table_split[:-1])
                except:
                    row['db_type'] = binlogevent.table
                row['db_name'] = binlogevent.schema

                topic = '%s.%s' %(binlogevent.schema, row['db_type'])

                now = datetime.utcnow()
                ltime = now.strftime("%Y-%m-%dT%H:%M:%S") + ".%03d" % (now.microsecond / 1000) + "Z"
                row['@timestamp'] = ltime
                row['db_binlog_file'] = self.current_binlog
                row['db_binlog_pos'] = binlogevent.packet.log_pos
                
                row['values'] = self.nil2null(row['values'])
                data = self.fmtjson(row)
                print data
                kfk.producer(topic, data)

    def nil2null(self, data):
        try:
            res = {}
            for k, v in data.iteritems(): 
                res[k] = v 
                if v == '':
                    res[k] = 'null'
                elif isinstance(v, bytes):
                    try:
                        json.dumps(v)
                    except:
                        res[k] = v.encode('hex')
            return res
        except:
            return data

    def fmtjson(self, row):
        try:
            return json.dumps(row, encoding='utf-8')
        except:
            for k, v in row.iteritems():
                if k in ['@timestamp', 'db_rows_event', 'db_type', 'db_name']: continue
                elif isinstance(v, bytes):
                    row[k] = v.encode('hex')
                elif isinstance(v, dict):
                    for key, val in v.iteritems():
                        if isinstance(val, bytes):
                            row[k][key] = val.encode('hex')
            return json.dumps(row, encoding='utf-8')


    def run(self):
        self.for_binlog()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("help: python binlogExport.py [db] [log_file] [log_pos]")
        sys.exit()
    table = sys.argv[1]
    log_file, log_pos = None, None
    if len(sys.argv) >= 4:
        log_file = sys.argv[2]
        log_pos = sys.argv[3]
    SyncBinlog(table, log_file, log_pos).run()
