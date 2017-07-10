#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from random import randint


class BasicTestSchdueler(object):
    def user_hash_input(self, input_data):
        return str(input_data)

    def user_process(self, input_data):
        return input_data * 1000


def validate_schduler_implement(scheduler):
    # Reset a scheduler
    scheduler.clear_all()
    scheduler.log_on()

    n = 20
    # First job, do 1/4 of entire job
    input_data_list = [randint(1, n) for _ in range(int(0.25 * n))]
    scheduler.do(
        input_data_list,
        quick_remove_duplicate=False,
        multiprocess=False,
    )

    # Second job, do everything
    input_data_list = [i for i in range(1, n + 1)]
    scheduler.do(
        input_data_list,
        quick_remove_duplicate=True,
        multiprocess=False,
    )

    assert len(scheduler) == n
    for key, output_data in scheduler.items():
        assert int(key) * 1000 == output_data
