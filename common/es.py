#coding:utf8
# Author: zh3linux(zenghuashan)

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import time
import logging, traceback
from elasticsearch import Elasticsearch
from config import config
from pyelasticsearch import ElasticSearch

class ES():
    def __init__(self):
        self.INDEX = 'qing'
        self.DOCTYPE = 'filesys'
        self.es = Elasticsearch(list(config.es.host), port=config.es.port)
        self.pyes = ElasticSearch('http://%s:%s' %(config.es.host, config.es.port))

    def set(self, esid, body):
        try:
            if not body:
                return False
            res = self.es.index(index=self.INDEX, doc_type=self.DOCTYPE, id=esid, body=body)
            return res
        except Exception, ex:
            logging.error("Err:%s -|- esid:%s -|- body:%s" %(ex, esid, str(body)))
            return False

    def delete(self, esid):
        try:
            res = self.es.delete(index=self.INDEX, doc_type=self.DOCTYPE, id=esid, ignore=[404])
            return res
        except Exception, ex:
            logging.error("Err:%s -|- esid:%s" %(ex, esid))
            return False

    def bulk(self, docs, index='qing', doc_type='filesys', esid="esid"):
        try:
            es_id = esid.split('-')
            if len(es_id) == 2:
                res = self.pyes.bulk((self.pyes.index_op(doc, id='%s-%s' %(doc.get(es_id[0]), doc.get(es_id[1]))) for doc in docs), index=index, doc_type=doc_type)
                return res
            else:
                res = self.pyes.bulk((self.pyes.index_op(doc, id=doc.get(esid)) for doc in docs),
                           index=index, doc_type=doc_type)
        except Exception, ex:
            logging.error(traceback.format_exc())
            logging.error("Err:%s -|- docs:%s" %(ex, str(docs)))
            time.sleep(1)
            self.bulk(docs, index=index, doc_type=doc_type, esid=esid)
            return False

es = ES()
