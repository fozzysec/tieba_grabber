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
from grab_setting import WORKERS,DBNAME,HOST,USER,PASSWORD,APP_NAME
from grab_post import grab_post
from queue import Queue
from concurrent.futures import ThreadPoolExecutor,wait

tp = ThreadPoolExecutor(max_workers=WORKERS)

def init_conn():
    conn = psycopg2.connect(dbname=DBNAME, host=HOST, user=USER, password=PASSWORD, application_name=APP_NAME)
    if not conn:
        sys.stderr.write('Unable to connect to pgsql server.\n')
        sys.exit(-1)
    else:
        return conn

def main(conf_file):
    with open(conf_file, 'r', encoding='utf8') as conf:
        doc = etree.parse(conf)
    session = init_session()
    items_queue = Queue()
    conn = init_conn()
    for keyword in doc.xpath('/xml/fuzz/@keyword'):
        cur = conn.cursor()
        cur.execute('CREATE TABLE IF NOT EXISTS %s (id serial PRIMARY KEY, data json NOT NULL, hash text UNIQUE NOT NULL);' % keyword)
        cur.execute('ALTER SEQUENCE %s_id_seq CYCLE;' % keyword)
        conn.commit()
        cur.close()
        print('[%s]grabbing index of %s' % (time.ctime(), keyword))
        items = grab_index(session, keyword)
        futures = []
        print('[%s]grabbing posts of %s' % (time.ctime(), keyword))
        for item in items:
            futures.append(tp.submit(grab_post, session, item, items_queue))
        wait(futures)
        sql_writer(conn, keyword, items_queue)
    conn.close()

if __name__ == '__main__':
    main(sys.argv[1])
