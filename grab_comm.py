import json
import psycopg2
import hashlib
from requests import Session
from requests.adapters import HTTPAdapter
from grab_setting import SITE_URL, USER_AGENT, RETRY_TIMES, TIMEOUT

def get_content(session, url, retries = RETRY_TIMES):
    if(retries <= 0):
        return
    try:
        res = session.get(url, timeout=TIMEOUT)
    except:
        return get_content(session, url, retries - 1)
    #res.encoding = 'utf8'
    return res

def init_session():
    session = Session()
    session.headers.update({'user-agent': USER_AGENT})
    session.mount(SITE_URL, HTTPAdapter(max_retries=RETRY_TIMES))
    return session

def sql_writer(conn, tablename, items_queue):
    print("Begin writing data into table %s" % tablename)
    cur = conn.cursor()
    while not items_queue.empty():
        checksum = hashlib.sha256()
        item_obj = items_queue.get()
        item = json.dumps(item_obj)
        contents = ''.join(item_obj['preview'])
        checksum.update(contents.encode('utf8'))
        cur.execute("INSERT INTO {} (data, hash) VALUES(%s, %s) ON CONFLICT (hash) DO NOTHING;".format(tablename), (item, checksum.hexdigest()))
    conn.commit()
    cur.close()


