#coding:utf8
# Author: zh3linux(zh3linux@gmail.com)
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

import logging, time

from kafka import SimpleProducer, KafkaClient
from kafka.common import LeaderNotAvailableError

from config import config

class KFK():
    def __init__(self):
        client = KafkaClient("%s:%s" %(config.kafka.host, config.kafka.port))
        print "%s:%s" %(config.kafka.host, config.kafka.port)
        self.simpleproducer = SimpleProducer(client)


    def producer(self, topic, msg):
        topic = b'%s' %topic
        msg = bytes(msg)
        #print msg
        try:
            self.simpleproducer.send_messages(topic, msg)
        except LeaderNotAvailableError:
            time.sleep(1)
            print "LeaderNotAvailableError"
            self.simpleproducer.send_messages(topic, msg)

kfk = KFK()

if __name__ == "__main__":
    kfk.producer('test', 'hello')
