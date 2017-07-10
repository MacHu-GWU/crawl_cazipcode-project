#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pickle
import hashlib


def input_data_fingerprint(input_data):
    """Fingerprint of the input data. Will be used in dupelicate detect.
    """
    m = hashlib.md5()
    m.update(pickle.dumps(input_data))
    return m.hexdigest()


if __name__ == "__main__":
    print(input_data_fingerprint(dict(a=1, b=2, c=3)))
