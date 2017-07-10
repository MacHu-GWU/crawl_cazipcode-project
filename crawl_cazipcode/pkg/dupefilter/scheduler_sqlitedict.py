#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from .pkg import sqlitedict
    from .base import Request, BaseDupeFilter
except:
    from dupefilter.pkg import sqlitedict
    from dupefilter.base import Request, BaseDupeFilter


class SqliteDictScheduler(BaseDupeFilter):
    user_db_path = None

    def __init__(self, logger=None):
        super(SqliteDictScheduler, self).__init__(logger=logger)

        # link encode method
        try:
            self.user_encode(None)
            encode = self.encode
        except NotImplementedError:
            encode = sqlitedict.encode
        except:
            encode = self.encode

        # link decode method
        try:
            self.user_decode(None)
            decode = self.decode
        except NotImplementedError:
            decode = sqlitedict.decode
        except:
            decode = self.decode

        # initiate back end database
        self._dct = sqlitedict.SqliteDict(
            self.user_db_path, autocommit=True,
            encode=encode,
            decode=decode,
        )

    @property
    def user_db_path(self):
        """Back-end sqlite database file path.
        """
        raise NotImplementedError

    def user_encode(self, obj):
        """Optional.

        :returns: bytes or string.

        **中文文档**

        用于对处理结果序列化的函数。默认使用pickle。
        """
        raise NotImplementedError

    def user_decode(self, bytes_or_str):
        """Optional.

        :returns: python object.

        **中文文档**

        用于对处理结果序列化的函数。默认使用pickle。
        """
        raise NotImplementedError

    def _is_duplicate(self, req):
        return req.key in self._dct

    def _quick_remove_duplicate(self, input_data_list):
        """

        **中文文档**

        移除那些key已经有对应的value的待处理数据。当且仅当判定数据是否被处理过
        的准则为: "key对应的value不为None" 的情况下有效。 
        """
        finished_key_set = set(self._dct.keys())
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
                yield req

    def _mark_finished(self, req):
        self._dct[req.key] = req.output_data

    def __len__(self):
        return len(self._dct)

    def __iter__(self):
        return iter(self._dct)

    def clear_all(self):
        self._dct.clear()

    def get_output(self, input_data):
        key = self.user_hash_input(input_data)
        return self._dct.get(key)

    def items(self):
        return self._dct.items()


if __name__ == "__main__":
    from dupefilter.tests.test_basic import (
        BasicTestSchdueler, validate_schduler_implement
    )

    def test():
        class Scheduler(BasicTestSchdueler, SqliteDictScheduler):
            user_db_path = ":memory:"

        scheduler = Scheduler()
        validate_schduler_implement(scheduler)

    test()
