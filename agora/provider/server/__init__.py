"""
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  This file is part of the Smart Developer Hub Project:
    http://www.smartdeveloperhub.org

  Center for Open Middleware
        http://www.centeropenmiddleware.com/
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Copyright (C) 2015 Center for Open Middleware.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

            http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
#-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=#
"""
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR

__author__ = 'Fernando Serena'

from flask import Flask, jsonify, request
from functools import wraps
from agora.provider.jobs.collect import add_triple_pattern, collect_fragment
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.redis import RedisJobStore
from datetime import datetime as dt, timedelta as delta

_batch_tasks = []
_after_collect_tasks = []

class APIError(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv


class NotFound(APIError):
    def __init__(self, message, payload=None):
        super(NotFound, self).__init__(message, 404, payload)


class Conflict(APIError):
    def __init__(self, message, payload=None):
        super(Conflict, self).__init__(message, 409, payload)


class AgoraApp(Flask):
    def __init__(self, name):
        super(AgoraApp, self).__init__(name)
        self.__handlers = {}
        self.errorhandler(self.__handle_invalid_usage)
        self._scheduler = BackgroundScheduler()
        # self._on_base_

    @staticmethod
    def __handle_invalid_usage(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

    def __scheduler_listener(self, event):
        print event
        self._scheduler.add_job(AgoraApp.batch_work, 'date', run_date=str(dt.now() + delta(seconds=10)))

    @classmethod
    def batch_work(cls):
        collect_fragment()
        for task in _batch_tasks:
            task()

    def run(self, host=None, port=None, debug=None, **options):
        jobstores = {
            'default': RedisJobStore(db=4, host=self.config['REDIS'])
        }
        executors = {
            'default': {'type': 'threadpool', 'max_workers': 20}
            # 'processpool': ProcessPoolExecutor(max_workers=5)
        }
        job_defaults = {
            # 'coalesce': False,
            # 'max_instances': 3
        }

        tasks = options.get('tasks', [])
        self._scheduler.configure(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
        self._scheduler.add_listener(self.__scheduler_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        for task in tasks:
            if task is not None and hasattr(task, '__call__'):
                _batch_tasks.append(task)

        after = options.get('after', [])
        for task in after:
            if task is not None and hasattr(task, '__call__'):
                _after_collect_tasks.append(task)
        self._scheduler.add_job(AgoraApp.batch_work, 'date', run_date=str(dt.now() + delta(seconds=1)))
        self._scheduler.start()
        super(AgoraApp, self).run(host='0.0.0.0', port=self.config['PORT'], debug=True, use_reloader=False)

    def __execute(self, f):
        @wraps(f)
        def wrapper():
            args, kwargs = self.__handlers[f.func_name](request)
            data = f(*args, **kwargs)
            response_dict = {'result': data, 'begin': int(kwargs['begin']), 'end': int(kwargs['end'])}
            if type(data) == list:
                response_dict['size'] = len(data)
            return jsonify(response_dict)
        return wrapper

    def __register(self, handler, pattern, collector):
        def decorator(f):
            self.__handlers[f.func_name] = handler
            if pattern is not None:
                for tp in pattern:
                    add_triple_pattern(tp, collector)
            return f
        return decorator

    def collect(self, path, handler, pattern, collector=None):
        def decorator(f):
            for dec in [self.__execute, self.__register(handler, pattern, collector), self.route(path)]:
                f = dec(f)
            return f

        return decorator
