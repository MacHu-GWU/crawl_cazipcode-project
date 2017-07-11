#!/usr/bin/env python
# -*- coding: utf-8 -*-

from crawl_cazipcode.pkg.dupefilter.scheduler_mongodb import MongoDBScheduler
from crawl_cazipcode.pkg.crawlib.spider import spider
from crawl_cazipcode.urlencoder import urlencoder
from crawl_cazipcode.htmlparser import htmlparser
from crawl_cazipcode.db import c_province


class Scheduler(MongoDBScheduler):
    """

    input_data::

        {
            "url": url,
        }
    """

    def user_hash_input(self, input_data):
        return input_data["url"]

    def user_process(self, input_data):
        url = input_data["url"]
        html = spider.get_html(url, encoding="iso-8859-1")
        return [city.to_dict() for city in htmlparser.parse_province(html)]

    def user_mark_finished(self, req):
        self._col.insert({"_id": req.key, "out": req.output_data})


province_key_list = [
    "nl",
    "ns",
    "pe",
    "nb",
    "qc",
    "on",
    "mb",
    "sk",
    "ab",
    "bc",
    "nu",
    "nt",
    "yt",
]

scheduler = Scheduler(collection=c_province)

# import webbrowser
# for province_key in province_key_list:
#     url = urlencoder.province(province_key)
#     webbrowser.open(url)


if __name__ == "__main__":
    input_data_list = [
        {"url": urlencoder.province(province_key)}
        for province_key in province_key_list
    ]
    scheduler.do(input_data_list, multiprocess=True)
