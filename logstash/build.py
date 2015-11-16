import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from jinja2 import Environment, FileSystemLoader
from config import config

logstash_path = os.path.dirname(os.path.realpath(__file__))
env = Environment(loader=FileSystemLoader(logstash_path))
template = env.get_template('kafka2es.conf')

os.makedirs(os.path.join(logstash_path, 'conf'))
for table,setting in config.tables.iteritems():
    schema = config.mysql.get(setting.get('db')).get('db')
    topic = '%s.%s' %(schema, table)
    index = setting.get('index')
    
    filename = '%s.conf' %topic
    with open(os.path.join(logstash_path, 'conf', filename), 'w') as f:
        f.write(template.render(config=config, topic=topic, index=index))



