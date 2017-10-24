#!/usr/bin/env python3

from gevent import monkey
monkey.patch_all()
import sys
import gevent
import time
import psycopg2
from lxml import etree
from grab_comm import init_session, sql_writer
from grab_index import grab_index
from grab_setting import WORKERS,DBNAME,HOST,USER,PASSWORD,APP_NAME,R_DBNAME, RHOST, R_USER, R_PASSWORD, R_APP_NAME, RPORT
from grab_post import grab_post
from queue import Queue
from concurrent.futures import ThreadPoolExecutor,wait

#tp = ThreadPoolExecutor(max_workers=WORKERS)

def init_conn():
    conn1 = psycopg2.connect(dbname=DBNAME, host=HOST, user=USER, password=PASSWORD, application_name=APP_NAME)
    conn2 = psycopg2.connect(dbname=R_DBNAME, host=RHOST, user=R_USER, password=R_PASSWORD, application_name=R_APP_NAME, port=RPORT)
    if not (conn1 and conn2):
        sys.stderr.write('Unable to connect to local pgsql server.\n')
        sys.exit(-1)
    else:
        return conn1, conn2

def main(conf_file):
    with open(conf_file, 'r', encoding='utf8') as conf:
        doc = etree.parse(conf)
    session = init_session()
    conn1, conn2 = init_conn()
    cur1 = conn1.cursor()
    cur2 = conn2.cursor()
    cur2.execute('CREATE TABLE IF NOT EXISTS records_all (id bigserial PRIMARY KEY, type text NOT NULL, data json NOT NULL, hash text UNIQUE NOT NULL);')
    cur2.execute('CREATE INDEX IF NOT EXISTS record_type_index ON records_all (type);')
    cur1.execute('CREATE TABLE IF NOT EXISTS records (id bigserial PRIMARY KEY, type text NOT NULL, data json NOT NULL, hash text UNIQUE NOT NULL);')
    cur1.execute('ALTER SEQUENCE records_id_seq CYCLE;')
    cur1.execute('CREATE INDEX IF NOT EXISTS records_type_index ON records (type);')
    conn1.commit()
    conn2.commit()
    cur2.close()
    cur1.close()
    conn2.close()
    for keyword in doc.xpath('/xml/fuzz/@keyword'):
        items_queue = Queue()
        print('[%s]grabbing index of %s' % (time.ctime(), keyword))
        items = grab_index(session, keyword)
        futures = []
        print('[%s]grabbing posts of %s' % (time.ctime(), keyword))
        for item in items:
            futures.append(gevent.spawn(grab_post, session, item, items_queue))
        #wait(futures)
        gevent.joinall(futures)
        sql_writer(conn1, keyword, items_queue)
    conn1.close()

if __name__ == '__main__':
    main(sys.argv[1])
