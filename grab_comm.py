import json
import psycopg2
import hashlib
import time
from requests import Session
from requests.adapters import HTTPAdapter
from grab_setting import SITE_URL, USER_AGENT, RETRY_TIMES, TIMEOUT, RHOST, RPORT, R_DBNAME, R_USER, R_PASSWORD, R_APP_NAME
from multiprocessing import Process
from http import cookiejar

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
    class BlockAll(cookiejar.CookiePolicy):
        return_ok = set_ok = domain_return_ok = path_return_ok = lambda self, *args, **kwargs: False
        netscape = True
        rfc2965 = hide_cookie2 = False

    session = Session()
    session.headers.update({'user-agent': USER_AGENT})
    session.mount(SITE_URL, HTTPAdapter(max_retries=RETRY_TIMES))
    session.cookies.set_policy(BlockAll())

    return session

def sql_writer(conn, keyword, items_queue):
    print("[%s]writing data into %s" % (time.ctime(), keyword))
    cur = conn.cursor()
    items = []
    while not items_queue.empty():
        checksum = hashlib.sha1()
        item_obj = items_queue.get()
        item = json.dumps(item_obj)
        contents = ''.join(item_obj['preview'])
        checksum.update(contents.encode('utf8'))
        cur.execute("INSERT INTO records (type, data, hash) VALUES(%s, %s, %s) ON CONFLICT (hash) DO NOTHING;", (keyword, item, checksum.hexdigest()))
        items.append((item, checksum.hexdigest()))
    conn.commit()
    cur.close()
    p = Process(target=remote_writer, args=(keyword, items))
    p.start()

def remote_writer(keyword, items):
    conn = psycopg2.connect(dbname=R_DBNAME, host=RHOST, user=R_USER, password=R_PASSWORD, application_name=R_APP_NAME, port=RPORT)
    if not conn:
        return
    cur = conn.cursor()
    for item, checksum in items:
        cur.execute("SELECT COUNT(*) FROM records_all WHERE hash = %s;", [checksum])
        rv = cur.fetchone()
        if rv[0] is not 0:
            continue
        else:
            cur.execute("INSERT INTO records_all (type, data, hash) VALUES(%s, %s, %s) ON CONFLICT (hash) DO NOTHING;", (keyword, item, checksum))
    conn.commit()
    cur.close()
    conn.close()
