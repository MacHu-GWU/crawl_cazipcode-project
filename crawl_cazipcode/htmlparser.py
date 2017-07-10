#!/usr/bin/env python
# -*- coding: utf-8 -*-

from crawl_cazipcode.pkg.dupefilter.pkg.sfm import nameddict
from crawl_cazipcode.pkg.crawlib.htmlparser import BaseHtmlParser


class Data(nameddict.Base):
    pass


class HtmlParser(BaseHtmlParser):
    def parse_province(self, html):
        data = list()

        soup = self.get_soup(html)
        for table in soup.find_all("table"):
            for a in table.find_all("a"):
                try:
                    href = a["href"]
                    if href.startswith("/canadian/city.asp?city="):
                        d = {s.split("=")[0]: s.split("=")[1]
                             for s in href.split("?")[-1].split("&")}
                        city_key = d["city"]
                        province_key = d["province"]

                        s = a.text
                        city, province = s.split(", ")

                        c = Data(
                            city_key=city_key,
                            province_key=province_key,
                            city=city,
                            province=province,
                        )
                        data.append(c)
                except Exception as e:
                    pass
        return data

    def parse_city(self, html):
        data = list()

        soup = self.get_soup(html)
        for table in soup.find_all("table"):
            for a in table.find_all("a"):
                try:
                    href = a["href"]
                    if href.startswith("/canadian/postal-code.asp?postalcode="):
                        postalcode_key = href.split("=")[-1]
                        postcode = a.text
                        p = Data(
                            postalcode_key=postalcode_key,
                            postcode=postcode,
                        )
                        data.append(p)
                except Exception as e:
                    pass
        return data

    def parse_postalcode(self, html):
        data = Data()

        soup = self.get_soup(html)
        for table in soup.find_all("table"):
            tr_list = table.find_all("tr")
            if len(tr_list) == 12:
                for tr in tr_list:
                    try:
                        td1, td2 = tr.find_all("td")
                        key = td1.text.strip().replace(":", "")
                        value = td2.text.strip()
                        setattr(data, key, value)
                    except:
                        pass

        if len(list(data.keys())):
            return data


htmlparser = HtmlParser()

if __name__ == "__main__":
    from pathlib_mate import Path
    from crawl_cazipcode.urlencoder import urlencoder
    from crawl_cazipcode.pkg.crawlib.spider import spider
    from crawl_cazipcode.pkg.dataIO.textfile import read, write
    from pprint import pprint

    _dir = "testhtml"

    def test_parse_province():
        province = "on"
        p = Path(_dir, "%s.html" % province)
        if not p.exists():
            url = urlencoder.province(province)
            html = spider.get_html(url, encoding="iso-8859-1")
            write(html, p.abspath)

        html = read(p.abspath)
        data = htmlparser.parse_province(html)
        pprint(data)

    test_parse_province()

    def test_parse_city():
        province = "on"
        city = "ottawa"
        p = Path(_dir, "%s.html" % city)
        if not p.exists():
            url = urlencoder.city(province, city)
            html = spider.get_html(url, encoding="iso-8859-1")
            write(html, p.abspath)

        html = read(p.abspath)
        data = htmlparser.parse_city(html)
        pprint(data)

    test_parse_city()

    def test_parse_postalcode():
        postcode = "k1a+0a1"
        p = Path(_dir, "%s.html" % postcode)
        if not p.exists():
            url = urlencoder.postalcode(postcode)
            html = spider.get_html(url, encoding="iso-8859-1")
            write(html, p.abspath)

        html = read(p.abspath)
        data = htmlparser.parse_postalcode(html)
        pprint(data)

    test_parse_postalcode()
