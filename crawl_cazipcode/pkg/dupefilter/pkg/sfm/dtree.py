#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""

**中文文档**

DictTree是类似于xml的一种文档树结构的实现。

- set attributes 操作为DictTree添加metadata, 例如 ``DictTree.name = "John"``
- set item 操作为DictTree添加子树, 例如 ``DictTree["Sam"] = DictTree(name="Sam")``
- for 循环操作为遍历所有的子树的key, 同理有 keys(), values(), items() 方法可以对
  key, value或者两者进行循环遍历。
- keys_at(), values_at(), items_at() 可以根据树的深度进行遍历。深度为0返回母树本身。
- length_at() 返回在某个深度上子树的数量。
- stats(), stats_at() 返回树的基本信息统计情况。

注意: 本实现性能很一般, 若你对性能有需求, 请使用xml文档树。本实现是面向对象的
实现方式, 根据同样的思路, 还有一个函数式的实现 :mod:`~sfm.dicttree`. 缺点是操作
比较不直观, 但是对于stats方法的性能有巨大的提升。这是因为在返回子树时候无需生成
子树对象。虽然生成子树对象实际上仅仅是将类的属性和字典进行绑定, 但是相对于在
循环中计数这一快速过程, 仍然是一笔不小的开销。
"""

import json
import pickle
import collections


class DictTree(object):
    """An pure python xml doc tree implementation in dictionary.

    **中文文档**

    internal data structure::

        {
            "__meta__": {key: value}, # parent tree attributes
            "child_key1": ... , # child tree's key, value pair.
            "child_key2": ... ,
            ...
        }

    对于根树而言, Key为 ``"root"``
    """
    __slots__ = ["__data__", ]

    def __init__(self, __data__=None, **kwargs):
        if __data__ is None:
            object.__setattr__(self, "__data__", {"__meta__": kwargs})
        else:
            object.__setattr__(self, "__data__", __data__)

    def __str__(self):
        try:
            return json.dumps(self.__data__, sort_keys=True, indent=4)
        except:
            return str(self.__data__)

    def dump(self, path):
        """dump DictTree data to json files.
        """
        try:
            with open(path, "wb") as f:
                f.write(self.__str__().encode("utf-8"))
        except:
            pass

        with open(path, "wb") as f:
            pickle.dump(self.__data__, f)

    @staticmethod
    def load(path):
        """load DictTree from json files.
        """
        try:
            with open(path, "rb") as f:
                return DictTree(__data__=json.loads(f.read().decode("utf-8")))
        except:
            pass

        with open(path, "rb") as f:
            return DictTree(__data__=pickle.load(f))

    def __getattribute__(self, attr):
        """
        """
        try:
            return object.__getattribute__(self, "__data__")["__meta__"][attr]
        except KeyError:
            return object.__getattribute__(self, attr)

    def __setattr__(self, attr, value):
        """
        """
        self.__data__["__meta__"][attr] = value

    def __setitem__(self, key, dicttree):
        if key == "__meta__":
            raise ValueError("'key' can't be '__meta__'!")

        if isinstance(dicttree, DictTree):
            self.__data__[key] = dicttree.__data__
        else:
            raise TypeError("attribute assignment only takes 'DictTree'.")

    def __getitem__(self, key):
        return DictTree(__data__=self.__data__[key])

    def __delitem__(self, key):
        if key == "__meta__":
            raise ValueError("'key' can't be '__meta__'!")

        del self.__data__[key]

    def __len__(self):
        """Return number of child trees.

        **中文文档**

        返回子树的数量。
        """
        return len(self.__data__) - 1

    def __iter__(self):
        for key in self.__data__:
            if key != "__meta__":
                yield key

    def keys(self):
        for key in self.__data__:
            if key != "__meta__":
                yield key

    def values(self):
        for key, value in self.__data__.items():
            if key != "__meta__":
                yield DictTree(__data__=value)

    def items(self):
        for key, value in self.__data__.items():
            if key != "__meta__":
                yield key, DictTree(__data__=value)

    def keys_at(self, depth, counter=1):
        if depth < 1:
            yield "__root__"
        else:
            if counter == depth:
                for key in self.keys():
                    yield key
            else:
                counter += 1
                for dicttree in self.values():
                    for key in dicttree.keys_at(depth, counter):
                        yield key

    def values_at(self, depth):
        if depth < 1:
            yield self
        else:
            for dicttree in self.values():
                for value in dicttree.values_at(depth - 1):
                    yield value

    def items_at(self, depth):
        if depth < 1:
            yield "__root__", self
        elif depth == 1:
            for key, value in self.items():
                yield key, value
        else:
            for dicttree in self.values():
                for key, value in dicttree.items_at(depth - 1):
                    yield key, value

    def length_at(self, depth):
        """Get the number of nodes on specific depth.
        """
        if depth == 0:
            return 1

        counter = 0
        for dicttree in self.values_at(depth - 1):
            counter += len(dicttree)
        return counter

    def stats(self, result=None, counter=0):
        """Display the node stats info on specific depth in this dict.

        ::

            [
                {"depth": 0, "leaf": M0, "root": N0},
                {"depth": 1, "leaf": M1, "root": N1},
                ...
                {"depth": k, "leaf": Mk, "root": Nk},
            ]
        """
        if result is None:
            result = dict()

        if counter == 0:
            if len(self):
                result[0] = {"depth": 0, "leaf": 0, "root": 1}
            else:
                result[0] = {"depth": 0, "leaf": 1, "root": 0}

        counter += 1
        if len(self):
            result.setdefault(
                counter, {"depth": counter, "leaf": 0, "root": 0})
            for dicttree in self.values():
                if len(dicttree):  # root
                    result[counter]["root"] += 1
                else:  # leaf
                    result[counter]["leaf"] += 1
                dicttree.stats(result, counter)

        return [
            collections.OrderedDict([
                ("depth", info["depth"]),
                ("leaf", info["leaf"]),
                ("root", info["root"]),
            ]) for info in sorted(result.values(), key=lambda x: x["depth"])
        ]

    def stats_at(self, depth):
        root, leaf = 0, 0
        for dicttree in self.values_at(depth):
            if len(dicttree):
                root += 1
            else:
                leaf += 1
        total = root + leaf
        print("On depth %s, having %s root nodes, %s leaf nodes. "
              "%s nodes in total." % (depth, root, leaf, total))


if __name__ == "__main__":
    def test_performance():
        path = "test.json"

        st = time.clock()
        d = DictTree(name=rand_str(8))
        for depth in range(6):
            for dicttree in d.values_at(depth):
                for _ in range(10):
                    dicttree[rand_str(8)] = DictTree(name=rand_str(8))
        print("creating elapse %.6f" % (time.clock() - st,))

        st = time.clock()
        d.dump(path)
        print("dumping elapse %.6f" % (time.clock() - st,))

        st = time.clock()
        d = DictTree.load(path)
        print("loading elapse %.6f" % (time.clock() - st,))

        st = time.clock()
        ppt(d.stats())
        print("analyze elapse %.6f" % (time.clock() - st,))

        st = time.clock()
        for depth in range(7):
            d.stats_at(depth)
        print("analyze elapse %.6f" % (time.clock() - st,))


# ....creating elapse 27.871160
# dumping elapse 23.268375
# loading elapse 3.634183
# [OrderedDict([('depth', 0), ('leaf', 0), ('root', 1)]),
#  OrderedDict([('depth', 1), ('leaf', 0), ('root', 10)]),
#  OrderedDict([('depth', 2), ('leaf', 0), ('root', 100)]),
#  OrderedDict([('depth', 3), ('leaf', 0), ('root', 1000)]),
#  OrderedDict([('depth', 4), ('leaf', 0), ('root', 10000)]),
#  OrderedDict([('depth', 5), ('leaf', 0), ('root', 100000)]),
#  OrderedDict([('depth', 6), ('leaf', 1000000), ('root', 0)])]
# analyze elapse 86.070060
# ...
