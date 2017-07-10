#!/usr/bin/env python
# -*- coding: utf-8 -*-

from crawl_cazipcode.pkg.crawlib.urlencoder import BaseUrlEncoder


class UrlEncoder(BaseUrlEncoder):
    domain = "https://www.zip-codes.com/"

    def province(self, province_key):
        href = "/canadian/province.asp?province={province_key}".format(
            province_key=province_key)
        return self.url_join(href)

    def city(self, province_key, city_key):
        href = "/canadian/city.asp?city={city_key}&province={province_key}".format(
            province_key=province_key, city_key=city_key,
        )
        return self.url_join(href)

    def postalcode(self, postalcode_key):
        href = "/canadian/postal-code.asp?postalcode={postalcode_key}".format(
            postalcode_key=postalcode_key)
        return self.url_join(href)


urlencoder = UrlEncoder()

if __name__ == "__main__":
    import webbrowser

    webbrowser.open(urlencoder.province("on"))
    webbrowser.open(urlencoder.city("on", "ottawa"))
    webbrowser.open(urlencoder.postalcode("k1a-0a1"))
