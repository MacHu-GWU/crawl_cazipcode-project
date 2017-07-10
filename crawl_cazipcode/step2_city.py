#!/usr/bin/env python
# -*- coding: utf-8 -*-

from crawl_cazipcode.pkg.dupefilter.scheduler_mongodb import MongoDBScheduler
from crawl_cazipcode.pkg.crawlib.spider import spider
from crawl_cazipcode.urlencoder import urlencoder
from crawl_cazipcode.htmlparser import htmlparser
from crawl_cazipcode.db import c_province, c_city


class Scheduler(MongoDBScheduler):
    """

    input_data:

        {
            "url": url,
            "city": city,
        }
    """

    def user_hash_input(self, input_data):
        return input_data["url"]

    def user_process(self, input_data):
        url = input_data["url"]
        html = spider.get_html(url, encoding="iso-8859-1")
        return [postalcode.to_dict() for postalcode in htmlparser.parse_city(html)]

    def user_mark_finished(self, req):
        self._col.insert(
            {"_id": req.key, "city": req.input_data["city"], "out": req.output_data})


scheduler = Scheduler(collection=c_city)

if __name__ == "__main__":
    input_data_list = list()

    url_set = set()
    for doc in c_province.find().limit(0):
        for city in doc["out"]:
            url = urlencoder.city(city["province_key"], city["city_key"])
            if url not in url_set:
                input_data = {"url": url, "city": city}
                input_data_list.append(input_data)
                url_set.add(url)

#     input_data_list = input_data_list[:10]
    scheduler.do(input_data_list, quick_remove_duplicate=True,
                 multiprocess=True)
