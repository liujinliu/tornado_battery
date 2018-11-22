# -*- coding: utf-8 -*-
#
#        H A P P Y    H A C K I N G !
#              _____               ______
#     ____====  ]OO|_n_n__][.      |    |
#    [________]_|__|________)<     |YANG|
#     oo    oo  'oo OOOO-| oo\\_   ~o~~o~
# +--+--+--+--+--+--+--+--+--+--+--+--+--+
#               Jianing Yang @  8 Feb, 2018
#

import asyncio


# 代码复制自peewee_async.TaskLocals
class TaskLocals:
    """Simple `dict` wrapper to get and set values on per `asyncio`
    task basis.
    The idea is similar to thread-local data, but actually *much* simpler.
    It's no more than a "sugar" class. Use `get()` and `set()` method like
    you would to for `dict` but values will be get and set in the context
    of currently running `asyncio` task.
    When task is done, all saved values are removed from stored data.
    """
    def __init__(self, loop):
        self.loop = loop
        self.data = {}

    def get(self, key, *val):
        """Get value stored for current running task. Optionally
        you may provide the default value. Raises `KeyError` when
        can't get the value and no default one is provided.
        """
        data = self.get_data()
        if data is not None:
            return data.get(key, *val)
        if val:
            return val[0]
        raise KeyError(key)

    def set(self, key, val):  # NOQA
        """Set value stored for current running task.
        """
        data = self.get_data(True)
        if data is not None:
            data[key] = val
        else:
            raise RuntimeError('No task is currently running')

    def get_data(self, create=False):
        """Get dict stored for current running task. Return `None`
        or an empty dict if no data was found depending on the
        `create` argument value.
        :param create: if argument is `True`, create empty dict
                       for task, default: `False`
        """
        task = asyncio.Task.current_task(loop=self.loop)
        if task:
            task_id = id(task)
            if create and task_id not in self.data:
                self.data[task_id] = {}
                task.add_done_callback(self.del_data)
            return self.data.get(task_id)
        return None

    def del_data(self, task):
        """Delete data for task from stored data dict.
        """
        del self.data[id(task)]
