#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pickle
try:
    from .pkg import sqlitedict
    from .base import Request, BaseDupeFilter
except:
    from dupefilter.pkg import sqlitedict
    from dupefilter.base import Request, BaseDupeFilter


class MongoDBScheduler(BaseDupeFilter):

    def __init__(self, logger=None, collection=None):
        super(MongoDBScheduler, self).__init__(logger=logger)

        self._col = collection

        # link encode method
        try:
            self.user_encode(None)
            self.encode = self.user_encode
        except NotImplementedError:
            self.encode = self._encode
        except:
            self.encode = self.user_encode

        # link decode method
        try:
            self.user_decode(None)
            self.decode = self.user_decode
        except NotImplementedError:
            self.decode = self._decode
        except:
            self.decode = self.user_decode

    def _encode(self, obj):
        return pickle.dumps(obj)

    def user_encode(self, obj):
        """Optional.

        :returns: bytes or string.

        **中文文档**

        用于对处理结果序列化的函数。默认使用pickle。
        """
        raise NotImplementedError

    def _decode(self, bytes_or_str):
        return pickle.loads(bytes_or_str)

    def user_decode(self, bytes_or_str):
        """Optional.

        :returns: python object.

        **中文文档**

        用于对处理结果序列化的函数。默认使用pickle。
        """
        raise NotImplementedError

    def _is_duplicate(self, req):
        return self._col.find_one({"_id": req.key}) is not None

    def _quick_remove_duplicate(self, input_data_list):
        """

        **中文文档**

        移除那些key已经有对应的value的待处理数据。当且仅当判定数据是否被处理过
        的准则为: "key对应的value不为None" 的情况下有效。 
        """
        finished_key_set = set([doc["_id"]
                                for doc in self._col.find({}, {"_id": True})])
        req_queue = list()

        nth_counter = 0
        for input_data in input_data_list:
            key = self.hash_input(input_data)
            if key not in finished_key_set:
                nth_counter += 1
                req = Request(
                    input_data=input_data,
                    key=key,
                    nth_counter=nth_counter,
                )
                finished_key_set.add(key)
                yield req

    def _mark_finished(self, req):
        self._col.insert({"_id": req.key, "out": self.encode(req.output_data)})

    def __len__(self):
        return self._col.find().count()

    def __iter__(self):
        return iter(self._dct)

    def clear_all(self):
        self._col.remove({})

    def get_output(self, input_data):
        key = self.user_hash_input(input_data)
        return self.decode(self._col.find_one({"_id", key})["out"])

    def items(self):
        for doc in self._col.find():
            yield (doc["_id"], self.decode(doc["out"]))


if __name__ == "__main__":
    import mongomock
    from dupefilter.tests.test_basic import (
        BasicTestSchdueler,
        validate_schduler_implement,
    )

    def test():
        class Scheduler(BasicTestSchdueler, MongoDBScheduler):
            pass

        scheduler = Scheduler(collection=mongomock.MongoClient().db.collection)
        validate_schduler_implement(scheduler)

    test()
