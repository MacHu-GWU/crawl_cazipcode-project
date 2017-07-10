#!/usr/bin/env python
# -*- coding: utf-8 -*-

from multiprocessing import cpu_count
from multiprocessing.dummy import Pool

try:
    from .util.fingerprint import input_data_fingerprint
    from .pkg.sfm import nameddict
    from .pkg.sfm.exception_mate import get_last_exc_info
    from .pkg.loggerFactory import StreamOnlyLogger
except:
    from dupefilter.util.fingerprint import input_data_fingerprint
    from dupefilter.pkg.sfm import nameddict
    from dupefilter.pkg.sfm.exception_mate import get_last_exc_info
    from dupefilter.pkg.loggerFactory import StreamOnlyLogger


class Request(nameddict.Base):
    __attrs__ = ["input_data", "key", "nth_counter", "left_counter",
                 "output_data"]

    def __init__(self,
                 input_data=None, key=None,
                 nth_counter=None, left_counter=None,
                 output_data=None,
                 context=None):
        self.input_data = input_data
        self.key = key
        self.nth_counter = nth_counter
        self.left_counter = left_counter
        self.output_data = output_data
        self.context = context


class BaseDupeFilter(object):
    """Base dupelicate filter abstract class.
    """

    def __init__(self, logger=None):
        if logger is None:
            self.logger = StreamOnlyLogger("Dupe Filter")
        else:
            self.logger = logger
        self.verbose = True

        # link hash input method
        try:
            self.user_hash_input(None)
            self.hash_input = self.user_hash_input
        except NotImplementedError:
            self.hash_input = self._hash_input
        except:
            self.hash_input = self.user_hash_input

        # link is duplicate method
        try:
            self.user_is_duplicate(None)
            self.is_duplicate = self.user_is_duplicate
        except NotImplementedError:
            self.is_duplicate = self._is_duplicate
        except:
            self.is_duplicate = self.user_is_duplicate

        # link mark finished method
        try:
            self.user_mark_finished(None)
            self.mark_finished = self.user_mark_finished
        except NotImplementedError:
            self.mark_finished = self._mark_finished
        except:
            self.mark_finished = self.user_mark_finished

    #--- customize method ---
    hash_input = None

    def _hash_input(self, input_data):
        """Default hash method to get a identical fingerprint for input data. 
        It as simple as: pickle the data and md5 it.

        This method will be used when :meth:`BaseDupeFilter.user_hash_input` 
        are not defined.

        :returns: string.        
        """
        return input_data_fingerprint(input_data)

    def user_hash_input(self, input_data):
        """User defined method to get a identical fingerprint for input data.

        :returns: string.
        """
        raise NotImplementedError

    def user_process(self, input_data):
        """Defines how to process the input data.
        """
        raise NotImplementedError

    def _process(self, req):
        """Hash input, process, mark as finished. Doesn't check duplication 
        here.

        **中文文档**

        处理数据。包含, 哈希, 处理, 标记完成三个步骤。
        """
        if req.left_counter is not None:
            self.info("Process %sth: %r, %s left ..." % (
                req.nth_counter, req.input_data, req.left_counter))
        else:
            self.info("Process %sth: %r ..." % (
                req.nth_counter, req.input_data))

        try:
            output_data = self.user_process(req.input_data)
            req.output_data = output_data
            self.mark_finished(req)
            self.info("Success!", 1)
        except Exception as e:
            self.info("Failed due to: %r" % get_last_exc_info(), 1)

    mark_finished = None

    def user_mark_finished(self, req):
        """User defined method to mark a request as "finished" after processed.
        """
        raise NotImplementedError

    def _mark_finished(self, req):
        """A method to mark a request as "finished" after processed.
        """
        raise NotImplementedError

    is_duplicate = None

    def _is_duplicate(self, req):
        """Check whether this request is already done.

        :returns: boolean. return True, when it's a duplicate item.
        """
        raise NotImplementedError

    def user_is_duplicate(self, req):
        """User defined method to check whether this request is already done.

        :returns: boolean. return True, when it's a duplicate item.
        """
        raise NotImplementedError

    def _quick_remove_duplicate(self, input_data_list):
        """

        :returns: req_queue, request queue

        **中文文档**

        在进行处理之前, 从输入数据中移除所有已经处理完成过的数据, 而不是使用
        默认设置, 在处理过程中, 检测是否已经完成过。这样做有助于大幅提高排重
        的效率。
        """
        nth_counter = 0
        for input_data in input_data_list:
            req = Request(
                input_data=input_data,
                key=self.hash_input(input_data),
            )
            if not self.is_duplicate(req):
                nth_counter += 1
                yield req

    #--- built-in method ---
    def do(self, input_data_list,
           quick_remove_duplicate=False,
           multiprocess=False):
        """Do the real work. Schedule, remove duplicate, process, and save.

        :param input_data_list: list of input data (or generator).
        :param quick_remove_duplicate: apply remove finished item (duplicate) 
          filter  before we start the real work.
        :param multiprocess: trigger to use multiprocess.
        """
        if quick_remove_duplicate:
            req_queue = self._quick_remove_duplicate(input_data_list)

        else:
            def generate_request(input_data_list):
                nth_counter = 0
                left_counter = len(input_data_list)
                for input_data in input_data_list:
                    nth_counter += 1
                    left_counter -= 1
                    req = Request(
                        input_data=input_data,
                        key=self.hash_input(input_data),
                        nth_counter=nth_counter,
                        left_counter=left_counter,
                        output_data=None,
                        context=None,
                    )
                    if not self.is_duplicate(req):
                        yield req

            req_queue = generate_request(input_data_list)
            self.info("Has %s item todo." % len(input_data_list))

        if multiprocess:
            self._do_multiprocess(req_queue)
        else:
            self._do(req_queue)

        self.info("Complete!")

    def _do(self, req_queue):
        """

        :param req_queue: request queue/list
        """
        for req in req_queue:
            self._process(req)

    def _do_multiprocess(self, req_queue):
        """

        :param req_queue: request queue/list
        """
        pool = Pool(processes=cpu_count())
        pool.map(self._process, req_queue)

    def clear_all(self):
        """Clear all data.

        **中文文档**

        重置Filter至初始状态。
        """
        raise NotImplementedError

    def __len__(self):
        """Number of finished items.
        """
        raise NotImplementedError

    def __iter__(self):
        """Iterate on all fingerprint of all finished items.
        """
        raise NotImplementedError

    def get(self, key):
        """Get output data by fingerprint of the input data.

        **中文文档**

        根据输入的指纹, 直接获得已经完成的输出数据。
        """
        raise NotImplementedError

    def get_output(self, input_data):
        """Get output data the input data.

        **中文文档**

        根据输入的数据, 直接获得已经完成的输出的数据。
        """
        return self.get(self.hash_input(input_data))

    def keys(self):
        """Return all fingerprint of all finished data.
        """
        return iter(self)

    def items(self):
        """Return fingerprint and output pairs of all finished data.
        """
        for key in self:
            yield (key, self.get(key))

    #--- Log ---
    def log_on(self):
        """Turn of logger.
        """
        self.verbose = True

    def log_off(self):
        """Turn off logger.
        """
        self.verbose = False

    def debug(self, msg, indent=0):
        """Log a message.
        """
        if self.verbose:
            self.logger.debug(msg, indent)

    def info(self, msg, indent=0):
        """Log a message.
        """
        if self.verbose:
            self.logger.info(msg, indent)

    def warning(self, msg, indent=0):
        """Log a message.
        """
        if self.verbose:
            self.logger.warning(msg, indent)

    def error(self, msg, indent=0):
        """Log a message.
        """
        if self.verbose:
            self.logger.error(msg, indent)

    def critical(self, msg, indent=0):
        """Log a message.
        """
        if self.verbose:
            self.logger.critical(msg, indent)
