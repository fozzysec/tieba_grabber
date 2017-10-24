from gevent import monkey
monkey.patch_all()
import re
import requests
from grab_setting import SITE_URL, QUERY_URL
from grab_comm import get_content
from lxml import html
from urllib.parse import quote_plus
from requests.adapters import HTTPAdapter

def grab_url(session, url):
    res = get_content(session, url)
    if res:
        doc = html.fromstring(res.text)
        for div in doc.xpath('//div[@class="s_post"]'):
            item = {}
            thread_url = div.xpath('.//span[@class="p_title"]/a[@class="bluelink"]/@href')[0]
            if('tieba.baidu.com' in thread_url):
                continue
            try:
                thread_title = div.xpath('.//span[@class="p_title"]/a/text()')[0]
                thread_author = div.xpath('.//a[not(@data-fid)]/font/text()')[0]
                thread_tieba = div.xpath('.//a[@class="p_forum"]/font/text()')[0]
                thread_date = div.xpath('.//font[@class="p_green p_date"]/text()')[0]
                thread_url = SITE_URL + thread_url
            except IndexError:
                continue
            item['url'] = thread_url
            item['title'] = thread_title
            item['author'] = thread_author
            item['tieba'] = thread_tieba
            item['date'] = thread_date
            yield item
        next_page = doc.xpath('//a[@class="next"]/@href')
        if next_page:
            yield from grab_url(session, SITE_URL + next_page[0])

def grab_index(session, keyword):
    return grab_url(session, QUERY_URL.format(SITE_URL, quote_plus(keyword)))
