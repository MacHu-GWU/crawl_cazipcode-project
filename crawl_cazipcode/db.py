#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pymongo

client = pymongo.MongoClient()
db = client.get_database("cazipcode")
c_province = db.get_collection("province")
c_city = db.get_collection("city")
c_postalcode = db.get_collection("postalcode")
