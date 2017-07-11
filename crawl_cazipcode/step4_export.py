#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
export crawled data to local file.
"""

import pandas as pd
from pprint import pprint
from crawl_cazipcode.pkg.superjson import json
from crawl_cazipcode.db import c_province, c_city, c_postalcode


def extract_int(text):
    res = list()
    for c in text:
        if c.isdigit():
            res.append(c)
    return int("".join(res))
    

def export():
    postalcode_data = list()
    for doc in c_postalcode.find().limit(0):
        data = dict()
        try:
            postalcode = doc["postalcode"]["postalcode"]
            data["postalcode"] = postalcode
        except:
            pass

        try:
            city = doc["city"]["city"].title()
            data["city"] = city
        except:
            pass

        try:
            province = doc["city"]["province"]
            data["province"] = province
        except:
            pass

        try:
            out = doc["out"]

            try:
                area_code = int(out["Area Code"])
                data["area_code"] = area_code
            except:
                pass

            try:
                area_name = out["Area Name"].title()
                data["area_name"] = area_name
            except:
                pass

            try:
                day_light_savings = out["Day Light Savings"]
                data["day_light_savings"] = day_light_savings == "Y"
            except:
                pass

            try:
                population = extract_int(out["Population"].replace(",", ""))
                data["population"] = population
            except:
                pass

            try:
                dwellings = extract_int(out["Dwellings"].replace(",", ""))
                data["dwellings"] = dwellings
            except:
                pass

            try:
                latitude = float(out["Latitude"])
                data["latitude"] = latitude
            except:
                pass

            try:
                longitude = float(out["Longitude"])
                data["longitude"] = longitude
            except:
                pass

            try:
                elevation = out["Elevation"]
                data["elevation"] = extract_int(elevation)
            except:
                pass

            try:
                timezone = int(out["Time Zone"])
                data["timezone"] = timezone
            except:
                pass
        except:
            pass
        print(data)
        postalcode_data.append(data)

    columns = [
        "postalcode", "city", "province", "area_code", "area_name",
        "latitude", "longitude", "elevation",
        "population", "dwellings", "timezone", "day_light_savings",
    ]
    df = pd.DataFrame(postalcode_data, columns=columns)
    df.to_csv("canada_postalcode.csv", index=False)
 
    json.safe_dump(postalcode_data, "canada_postcode.json", verbose=False)


def export_database():
    """Export entire crawled data from database to local.
    """
    province_data = list(c_province.find())
    json.safe_dump(province_data, "province_data.json.gz")
    
    city_data = list(c_city.find())
    json.safe_dump(city_data, "city_data.json.gz")
    
    postalcode_data = list(c_postalcode.find())
    json.safe_dump(postalcode_data, "postalcode_data.json.gz")


if __name__ == "__main__":
    export()
#     export_database()
