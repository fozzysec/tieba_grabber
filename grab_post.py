import requests
import sys
from lxml import html
from grab_comm import get_content

def grab_post(session, item, items_queue):
    if not item:
        return
    anchor = item['url'].split('#')[1]
    res = get_content(session, item['url'])
    if res:
        doc = html.fromstring(res.text)
        item['preview'] = []
        contents = doc.xpath('//div[@id="post_content_%s"]/text()' % anchor)
        if not contents:
            sys.stderr.write('Not reply, discard it.\n')
            return
        for row in contents:
            item['preview'].append(row)
        item['preview'] = '<br>'.join(item['preview'])
        items_queue.put(item)
