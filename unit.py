#!/usr/bin/python3
__author__ = 'ziyan.yin'

import time
from typing import Any, Dict


class Unit:
    """
        basic process structure
            >>> data = {}
            >>> operator = '1'
            >>> ip = "127.0.0.1"
            >>> p = Unit(data, operator, ip)
            >>> p.execute()
            <dict: {'code':0, 'success':True, 'data': [], 'message': ''}
    """

    log = False
    level = 1

    error_code = {
        '10001': '访问失败',
        '10002': '非法访问',
        '10003': '访问次数过多',
        '10004': '参数错误',
        '10005': '服务器错误',
        '10006': '数据库错误',
        '10007': '其他错误',
    }

    _risk = set()

    def __init__(self, content, operator: str = '', ip: str = ''):
        self.content = content
        self.operator = operator
        self.ip = ip

        self.error: str = ''
        self.data: Dict[Any] = dict()

    def init(self):
        if not self.operator:
            self.error = Unit.error_msg('10002', '请稍后再试')
            return False
        if self.ip in self.__class__._risk:
            self.error = Unit.error_msg('10003', '请稍后再试')
            return False
        return True

    def before_validate(self):
        pass  # user apply for dev service

    def validate(self):
        return True

    def after_validate(self):
        pass  # user apply for dev service

    def before_process(self):
        pass  # user apply for dev service

    def process(self):
        return True

    def after_process(self):
        pass  # user apply for dev service

    def execute(self):
        if not self.init():
            if self.log:
                self.logger(False, self.error)
            return Unit.format(False, [], str(self.error))
        return self._execute()

    def _execute(self):
        try:
            self.before_validate()
            if not self.validate():
                if self.log:
                    self.logger(False, self.error)
                return Unit.format(False, [], str(self.error))
            self.after_validate()
            self.before_process()
            if not self.process():
                if self.log:
                    self.logger(False, self.error)
                return Unit.format(False, [], str(self.error))
            self.after_process()
            if self.log:
                self.logger(True, '')
            return Unit.format(True, self.data, '')
        except Exception as ex:
            self.error = str(ex)
            if self.log:
                self.logger(False, self.error)
            return Unit.format(False, [], str(self.error))

    def logger(self, result, msg):
        raise NotImplementedError()

    @staticmethod
    def format(success: bool = True, data: Any = None, msg: str = '', code: int = 0):
        return {
            "code": int(code),
            "success": success,
            "data": data,
            "message": str(msg)
        }

    @staticmethod
    def error_msg(code, msg):
        return '[{0}]{1}'.format(code, msg)


units: Dict[str, Dict[str, Unit]] = dict()
