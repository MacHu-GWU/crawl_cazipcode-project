#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
export crawled data to local file.
"""

import random
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
            data["postalcode"] = None

        try:
            city = doc["city"]["city"].title()
            data["city"] = city
        except:
            data["city"] = None

        try:
            province = doc["city"]["province"]
            data["province"] = province
        except:
            data["province"] = None

        try:
            out = doc["out"]

            try:
                area_code = int(out["Area Code"])
                data["area_code"] = area_code
            except:
                data["area_code"] = None

            try:
                area_name = out["Area Name"].title()
                data["area_name"] = area_name
            except:
                data["area_name"] = None

            try:
                day_light_savings = out["Day Light Savings"]
                data["day_light_savings"] = int(day_light_savings == "Y")
            except:
                data["day_light_savings"] = None

            try:
                population = extract_int(out["Population"].replace(",", ""))
                data["population"] = population
            except:
                data["population"] = None

            try:
                dwellings = extract_int(out["Dwellings"].replace(",", ""))
                data["dwellings"] = dwellings
            except:
                data["dwellings"] = None

            try:
                latitude = float(out["Latitude"])
                data["latitude"] = latitude
            except:
                data["latitude"] = None

            try:
                longitude = float(out["Longitude"])
                data["longitude"] = longitude
            except:
                data["longitude"] = None

            try:
                elevation = out["Elevation"]
                data["elevation"] = extract_int(elevation)
            except:
                data["elevation"] = None

            try:
                timezone = int(out["Time Zone"])
                data["timezone"] = timezone
            except:
                data["timezone"] = None
        except:
            pass

        postalcode_data.append(data)

    columns = [
        "postalcode", "city", "province", "area_code", "area_name",
        "latitude", "longitude", "elevation",
        "population", "dwellings", "timezone", "day_light_savings",
    ]

    # csv with full data
    df = pd.DataFrame(postalcode_data, columns=columns)
    df.to_csv("canada_postalcode.csv", index=False)

    # csv for demo
    df = pd.DataFrame(random.sample(postalcode_data, 200), columns=columns)
    df.to_csv("canada_postalcode_demo.csv", index=False)

    # json with full data, compressed (IMPORTANT!)
    json.safe_dump(postalcode_data, "canada_postalcode.json.gz", verbose=False)

    # json for demo
    json.safe_dump(
        random.sample(postalcode_data, 200), "canada_postalcode_demo.json",
        pretty=True, verbose=False)


def export_database():
    """Export entire crawled data from database to local.
    """
    province_data = list(c_province.find())
    json.safe_dump(province_data, "province_data.json.gz")

    city_data = list(c_city.find())
    json.safe_dump(city_data, "city_data.json.gz")

    postalcode_data = list(c_postalcode.find())
    json.safe_dump(postalcode_data, "postalcode_data.json.gz")


def export_to_cazipcode_database():
    from sqlalchemy import String, Integer, Float
    from sqlalchemy import create_engine, MetaData, Table, Column, Index

    engine = create_engine("sqlite:///data.sqlite")
    metadata = MetaData()

    t = Table("canada_postalcode", metadata,
              Column("postalcode", String, primary_key=True),
              Column("city", String),
              Column("province", String),
              Column("area_code", Integer),
              Column("area_name", String),
              Column("latitude", Float),
              Column("longitude", Float),
              Column("elevation", Integer),
              Column("population", Integer),
              Column("dwellings", Integer),
              Column("timezone", Integer),
              Column("day_light_savings", Integer),
              )

    metadata.create_all(engine)

    ins = t.insert()

    postalcode_data = json.load("canada_postalcode.json.gz", verbose=False)
    postalcode_data = sorted(postalcode_data, key=lambda p: p["postalcode"])

    engine.execute(ins, postalcode_data)

    i_city = Index("c_city", t.c.city)
    i_city.create(engine)

    i_province = Index("c_province", t.c.province)
    i_province.create(engine)

    i_latitude = Index("c_latitude", t.c.latitude)
    i_latitude.create(engine)

    i_longitude = Index("c_longitude", t.c.longitude)
    i_longitude.create(engine)

    i_population = Index("c_population", t.c.population)
    i_population.create(engine)

    i_dwellings = Index("c_dwellings", t.c.dwellings)
    i_dwellings.create(engine)

    i_timezone = Index("c_timezone", t.c.timezone)
    i_timezone.create(engine)


def query():
    for doc in c_postalcode.find():
        postalcode = doc["postalcode"]["postalcode"]
        if postalcode.startswith("J8L"):
            print(doc)


if __name__ == "__main__":
    export()
    export_database()
    export_to_cazipcode_database()

#     query()
    print("Complete!")
