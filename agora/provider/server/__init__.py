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
from agora.provider.jobs.collect import collect_fragment
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.redis import RedisJobStore
from datetime import datetime as dt, timedelta as delta

_batch_tasks = []

# Configuration dictionary that will be populated on application run
config = {}

class APIError(Exception):
    """
    Exception class to raise when an API request is not valid
    """
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
    """
    404 response class
    """
    def __init__(self, message, payload=None):
        super(NotFound, self).__init__(message, 404, payload)


class Conflict(APIError):
    """
    409 response class
    """
    def __init__(self, message, payload=None):
        super(Conflict, self).__init__(message, 409, payload)


class AgoraApp(Flask):
    """
    Provider base class for the Agora services
    """
    def __init__(self, name, config_class):
        """
        :param name: App name
        :param config_class: String that represents the config class to be used
        :return:
        """
        super(AgoraApp, self).__init__(name)
        self.__handlers = {}
        self.errorhandler(self.__handle_invalid_usage)
        self._scheduler = BackgroundScheduler()
        self.config.from_object(config_class)

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
        """
        Method to be executed in batch mode for collecting the required fragment (composite)
        and then other custom tasks.
        :return:
        """
        collect_fragment()
        for task in _batch_tasks:
            task()

    def run(self, host=None, port=None, debug=None, **options):
        """
        Start the AgoraApp expecting the provided config to have at least REDIS and PORT fields.
        """
        jobstores = {
            'default': RedisJobStore(db=4, host=self.config['REDIS'])
        }
        executors = {
            'default': {'type': 'threadpool', 'max_workers': 20}
        }

        tasks = options.get('tasks', [])
        self._scheduler.configure(jobstores=jobstores, executors=executors, job_defaults={})
        self._scheduler.add_listener(self.__scheduler_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        for task in tasks:
            if task is not None and hasattr(task, '__call__'):
                _batch_tasks.append(task)

        self._scheduler.add_job(AgoraApp.batch_work, 'date', run_date=str(dt.now() + delta(seconds=1)))
        self._scheduler.start()
        super(AgoraApp, self).run(host='0.0.0.0', port=self.config['PORT'], debug=True, use_reloader=False)

    def __execute(self, f):
        @wraps(f)
        def wrapper():
            args, kwargs = self.__handlers[f.func_name](request)
            data = f(*args, **kwargs)
            begin = int(kwargs['begin'])
            if isinstance(data, tuple):
                begin = data[0]
                data = data[1]
            response_dict = {'result': data, 'begin': begin, 'end': int(kwargs['end'])}
            if type(data) == list:
                response_dict['size'] = len(data)
            return jsonify(response_dict)
        return wrapper

    def __register(self, handler):
        def decorator(f):
            self.__handlers[f.func_name] = handler
            return f
        return decorator

    def register(self, path, handler):
        def decorator(f):
            for dec in [self.__execute, self.__register(handler), self.route(path)]:
                f = dec(f)
            return f
        return decorator
