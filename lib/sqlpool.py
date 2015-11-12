#!/usr/bin/python
#coding:utf8

import os
import _mysql
import time, logging
import threading
from _mysql_exceptions import IntegrityError, OperationalError
from threading import current_thread

#ERR_SERVER_GONEAWAY = 2006      # (2006, 'MySQL server has gone away')
#ERR_LOST_CONN_IN_QUERY = 2013   # (2013, 'Lost connection to MySQL server during query')
ERR_SYNTAX_ERROR = 1149

def escape(var):
    '''这里假定连接数据库都是使用utf8的。'''
    if var is None:
        return ''
    if isinstance(var, unicode):
        var = var.encode('utf8')
    if not isinstance(var, str):
        var = str(var)
    return _mysql.escape_string(var)

class _SqlConn():
    def __init__(self, conn):
        '''应该由SqlPool的getConn方法去初始化本类'''
        self.conn = conn

    def __del__(self):
        pass

    def execute(self, sql):
        '''@return: 成功返回影响行数,失败返回 小于零的整数.-2为主键冲突,其它负数未定义错误类型.'''
        try:
            self.conn.query(sql)
            res = self.conn.affected_rows()
            if res < 0 or res == 0xFFFFFFFFFFFFFFFF:
                # ps : 0xFFFFFFFFFFFFFFFF (64位的-1) 
                # TODO 这个值与驱动、系统、硬件CPU位数都可能有关
                logging.error('mysql execute error (err=%d) : %s' % (res, sql))
            return res

        except IntegrityError, e:
            logging.error('mysql execute error (err=%s) : %s' % (str(e), sql))
            return (SqlPool.KEY_ERROR) #发生主键冲突

    def query(self, sql, how=1):
        '''执行一条查询的SQL语句，返回查询结果,也可以马上调用result方法对能取得. 每次的query对应一次的result.
        @param how:
            0 -- tuples (default), RET_TUPLE
            1 -- dictionaries, key=column or table.column if duplicated
            2 -- dictionaries, key=table.column.'''
        
        #assert (sql).lower().startswith("select") == True or (sql).lower().startswith("show") == True, 'must readonly query.'
        self.conn.query(sql)
        res = self.conn.store_result()
        if res:
            return res.fetch_row(res.num_rows(), how)
        else:
            return ()

class SqlPool(threading.Thread):
    '''全局线程变量，继承于local类'''
    KEY_ERROR = -2
    RET_TUPLE = 0
    RET_DICT_ROW_FOR_KEY = 1
    RET_DICT_TABLE_ROW_FOR_KEY = 2
    
    __mutex = threading.Lock()
    __remotes = {}
    __remotes_initial = {}
    
    def __new__(cls, host, user, passwd, db, port=18889):
        with SqlPool.__mutex:
            svrIdent = "%s:%s:%s" % (host, port, db)
            obj = SqlPool.__remotes.get(svrIdent)
            if obj is None:
                obj = SqlPool.__remotes[svrIdent] = object.__new__(cls)        
        return obj
    
    def __init__(self, host, user, passwd, db, port=18889):
        with SqlPool.__mutex:
            svrIdent = "%s:%s:%s" % (host, port, db)
            if not SqlPool.__remotes_initial.get(svrIdent):
                SqlPool.__remotes_initial[svrIdent] = True
                threading.Thread.__init__(self, target = self._threadChecker, name = 'sqlpool')
                
                self._db = db
                self._host = host
                self._user = user
                self._passwd = passwd
                self._port = int(port)
                
                #self._pid = os.getpid()
                self.__conns = {}
                self.__wrap_conns = {}
                
                self.setDaemon(True)
                self.start()
    
    @property
    def _connections(self):
        return self.__conns.setdefault(os.getpid(), {})
    
    @property
    def _wrapConns(self):
        return self.__wrap_conns.setdefault(os.getpid(), {})
    
    def __del__(self):
        self._disconnect()
    
    def _disconnect(self):
        pid = os.getpid()
        self.__wrap_conns.pop(pid, None)
        conns = self.__conns.pop(pid, {})
        for conn in conns.itervalues():
            try: conn.close()
            except: pass
    
#    def _checkPid(self):
#        nowPid = os.getpid()
#        if self._pid != nowPid:
#            self._pid = nowPid
#            # self._disconnect()    # 暂时先不影响其它链接，因为有可能其它进程在shi
#        return self._pid
    
    def _threadChecker(self):
        while True:
            try:
                # for those :
                #     1. might request thread is closed
                #     2. cause by bug ...
                actives = set(thread.ident for thread in threading.enumerate())
                keepings = set(ident for ident in self._connections.keys())

                useless = keepings - actives
                if useless:
                    logging.warning('sqlpool : useless connection found (%d)' % len(useless))
                
                # release useless connection
                for ident in useless:
                    for thread in threading.enumerate():
                        if thread.ident == ident and thread.isAlive():
                            break
                    else:
                        self.__releaseConnection(ident)
            
            except Exception, e:
                logging.error('sqlpool error (_threadChecker) : %s' % str(e))
            
            finally:
                # self._saveStatus()
                time.sleep(30 * 60)
    
    def _saveStatus(self):
        try:
#            fpath = os.path.abspath(__file__)
#            with open(fpath, "ab") as fd:
#                fd.write("(sqlpool) %s" % self.dumpStatus())
            logging.error("(sqlpool) %s" % self.dumpStatus())        
        except:
            pass
    
    def dumpStatus(self):
        info = {}
        info["py_pid"] = os.getpid()
        info["conn_count"] = len(self._connections)
        info["runtime_threads"] = threads = [thread.ident for thread in threading.enumerate()]
        info["Thread_2_ConnId"] = thread2ConnId = {}
        
        threads.sort()
        for ident, conn in self._connections.iteritems():
            thread2ConnId[ident] = conn.thread_id()
        
        return info
    
    # - raw conn
    def __newConnection(self):
        conn = _mysql.connect(
            db=self._db,
            host=self._host,
            user=self._user,
            passwd=self._passwd,
            port=self._port)
        
        conn.set_character_set('utf8')
        return conn
    
    def __closeRawConnection(self, ident):
        try:
            conn = self._connections.pop(ident, None)
            if conn is not None:
                conn.close()
        except Exception, e:
            logging.error('sqlpool error (_releaseConnection) : %s' % str(e))
    
    def __reconnectRaw(self, ident):
        self.__closeRawConnection(ident)
        conn = self.__newConnection()
        self._connections[ident] = conn
        return conn
    
    # - wrap conn
    def __releaseConnection(self, ident):
        self._wrapConns.pop(ident, None)
        self.__closeRawConnection(ident)
    
    def __reconnect(self, ident):
        self._wrapConns.pop(ident, None)
        conn = self.__reconnectRaw(ident)
        if conn is not None:
            wrapConn =  _SqlConn(conn)
            self._wrapConns[ident] = wrapConn
            return wrapConn
    
    # - wrap conn by thread respectively
    def _getConn(self):
        '''获取连接'''
        ident = current_thread().ident
        wrapConn = self._wrapConns.get(ident)
        if wrapConn is None:
            wrapConn = self.__reconnect(ident)
        
        return wrapConn
    
    def _getNewConn(self):
        ident = current_thread().ident
        return self.__reconnect(ident)
    
#    def _valid_connection(self, ident):
#        conn = self._connections.get(ident)    
#        if conn:
#            try:
#                conn.ping()
#                return True
#            except:
#                self._releaseConnection(ident)
#
#        return False
    
    def closeConn(self, conn):
        '''关闭连接'''
        pass
    
    def execute(self, sql):
        '''执行语句, 返回影响行数'''
        conn = self._getConn()
        try:
            return conn.execute(sql)
        except OperationalError, err:
            if err[0] != ERR_SYNTAX_ERROR:
                conn = self._getNewConn()
                try:
                    return conn.execute(sql)
                except Exception, e:
                    err = e
            
            logging.error('mysql execute error (err=%s) : %s' % (str(err), sql))
            raise err
        
        except Exception, e:
            logging.error('mysql execute error (err=%s) : %s' % (str(e), sql))
            raise e
    
    def executeReturnInsertId(self, sql):
        '''执行语句, 返回影响行数及LAST_INSERT_ID'''
        conn = self._getConn()
        insertResult = None
        try:
            insertResult = conn.execute(sql)
        except OperationalError, err:
            if err[0] != ERR_SYNTAX_ERROR:
                conn = self._getNewConn()
                try:
                    return conn.execute(sql)
                except Exception, _e:
                    err = _e
            
            logging.error('mysql execute_with_id error (err=%s) : %s' % (str(err), sql))
            raise err
        
        except Exception, e:
            logging.error('mysql execute_with_id error (err=%s) : %s' % (str(e), sql))
            raise e

        if insertResult is None or insertResult < 1:
            return (insertResult, 0)

        # get id
        ''' NOTICE: 必须使用同一个conn调用LAST_INSERT_ID()。
                    为了兼容mysql-proxy, LAST_INSERT_ID和insert语句之间不能插入其他任何语句。
        '''
        try:
            retData = conn.query('SELECT LAST_INSERT_ID()', 0)
        except OperationalError, e:
            logging.error('mysql LAST_INSERT_ID error (err=%s) : %s' % (str(e), sql))
            raise e

        if not retData or not retData[0] or not retData[0][0]:
            raise Exception('query LAST_INSERT_ID return None.[%s]' % sql)
        
        insertId = retData[0][0]
        return (insertResult, int(insertId))

    def query(self, sql, how=1):
        '''执行语句，返回查询结果'''
        conn = self._getConn()
        try:
            return conn.query(sql, how)
        except OperationalError, err:
            if err[0] != ERR_SYNTAX_ERROR:
                conn = self._getNewConn()
                try:
                    return conn.query(sql, how)
                except Exception, _e:
                    err = _e
            
            logging.error('mysql query error (err=%s) : %s' % (str(err), sql))
            raise err
        
        except Exception, e:
            logging.error('mysql query error (err=%s) : %s' % (str(e), sql))
            raise e

class SqlUtil:
    @staticmethod
    def getFieldsDefaultValue(sqlPool, table):
        res = sqlPool.query('desc %s;' % table, SqlPool.RET_DICT_ROW_FOR_KEY)
        if res:
            defVals = {}
            for row in res:
                if row['Extra'] == 'auto_increment':
                    continue

                field, defVal, fieldType = row['Field'], row['Default'], row['Type'].lower()
                if defVal != None:
                    res = sqlPool.query('select default(`%s`) as df from %s limit 1;' % (field, table),
                                            SqlPool.RET_DICT_ROW_FOR_KEY)
                    defVal = res[0]['df'] if res else defVal

                # caution ! here may cause bugs ...
                if (defVal is None) and row['Null'] == 'NO' and fieldType.find('char') != 0:
                    defVal = ''
                elif defVal is not None:
                    if fieldType.find('int') != -1: defVal = int(defVal)

                defVals[row['Field']] = defVal

            return defVals

# 每一个Dao都设一个假cache，如果使用memcache，在__initialize__时
class FakeCache(object):
    def get(self, key):
        return None
    def set(self, key, val, time=0):
        return None
    def delete(self, key):
        return None

class DaoBase():
    def __init__(self, host, username, password, database, table):
        self._table = escape(table)
        self._sqlpool = SqlPool(host, username, password, database)
        self.cache = FakeCache()

    def dropTable(self):
        self._sqlpool.execute('DROP TABLES IF  EXISTS `%s`;' % self._table)

    def deploy(self):
        return self._sqlpool.execute(self._getTableStruct() % self._table)

if __name__ == '__main__':

    pool = SqlPool(
        host='10.20.188.113',
        port=3306,
        user='qing',
        passwd='admin',
        db='qing')
    
    print pool.query('show tables', SqlPool.RET_DICT_ROW_FOR_KEY)
    print pool.query('select * from qing_group limit 1', 1)
    print pool.dumpStatus()
    
    pool2 = SqlPool(
        host='10.20.187.99',
        port=3306,
        user='qing',
        passwd='admin',
        db='qing')
    
    print pool2.query('show tables', SqlPool.RET_DICT_ROW_FOR_KEY)
    print pool2.query('select * from qing_group limit 1', 1)
    print pool2.dumpStatus()
