#!/usr/bin/env python
# -*- coding: utf-8 -*-

from crawl_cazipcode.pkg.dupefilter.scheduler_mongodb import MongoDBScheduler
from crawl_cazipcode.pkg.crawlib.spider import spider
from crawl_cazipcode.urlencoder import urlencoder
from crawl_cazipcode.htmlparser import htmlparser
from crawl_cazipcode.db import c_province, c_city, c_postalcode


class Scheduler(MongoDBScheduler):
    """

    input_data::

        {
            "url": url,
            "city": city,
            "out": out,
        }
    """

    def user_hash_input(self, input_data):
        return input_data["url"]

    def user_process(self, input_data):
        url = input_data["url"]
        html = spider.get_html(url, encoding="iso-8859-1")
        return htmlparser.parse_postalcode(html).to_dict()

    def user_mark_finished(self, req):
        self._col.insert({
            "_id": req.key,
            "city": req.input_data["city"],
            "postalcode": req.input_data["postalcode"],
            "out": req.output_data,
        })


scheduler = Scheduler(collection=c_postalcode)

if __name__ == "__main__":
    input_data_list = list()

    url_set = set()
    for doc in c_city.find().limit(0):
        city = doc["city"]
        for postalcode in doc["out"]:
            postalcode_key = postalcode["postalcode_key"]
            url = urlencoder.postalcode(postalcode_key)
            if url not in url_set:
                input_data = {"url": url, "city": city,
                              "postalcode": postalcode}
                input_data_list.append(input_data)
                url_set.add(postalcode_key)

#     input_data_list = input_data_list[:10]
    scheduler.do(input_data_list, quick_remove_duplicate=True,
                 multiprocess=True)
