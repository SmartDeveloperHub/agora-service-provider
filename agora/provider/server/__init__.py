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
__author__ = 'Fernando Serena'

from flask import Flask, jsonify, request, make_response
from functools import wraps
from agora.provider.jobs.collect import collect_fragment
from threading import Thread, Event
import time

_batch_tasks = []

# Configuration dictionary that will be populated on application run
config = {}


def get_accept():
    return str(request.accept_mimetypes).split(',')


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
        self.__rdfizers = {}
        self.errorhandler(self.__handle_invalid_usage)
        self.config.from_object(config_class)
        self._stop_event = Event()

    @staticmethod
    def __handle_invalid_usage(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

    def batch_work(self):
        """
        Method to be executed in batch mode for collecting the required fragment (composite)
        and then other custom tasks.
        :return:
        """
        while True:
            collect_fragment(self._stop_event)
            for task in _batch_tasks:
                task(self._stop_event)
            time.sleep(10)
            if self._stop_event.isSet():
                return

    def run(self, host=None, port=None, debug=None, **options):
        """
        Start the AgoraApp expecting the provided config to have at least REDIS and PORT fields.
        """

        tasks = options.get('tasks', [])
        for task in tasks:
            if task is not None and hasattr(task, '__call__'):
                _batch_tasks.append(task)

        thread = Thread(target=self.batch_work)
        thread.start()
        try:
            super(AgoraApp, self).run(host='0.0.0.0', port=self.config['PORT'], debug=True, use_reloader=False)
        except Exception, e:
            print e.message
        self._stop_event.set()
        if thread.isAlive():
            thread.join()

    def __execute(self, f):
        @wraps(f)
        def wrapper():
            mimes = get_accept()
            if 'application/json' in mimes:
                args, kwargs = self.__handlers[f.func_name](request)
                context, data = f(*args, **kwargs)
                response_dict = {'context': context, 'result': data}
                return jsonify(response_dict)
            else:
                response = make_response(self.__rdfizers[f.func_name](f.func_name).serialize(format='turtle'))
                response.headers['Content-Type'] = 'text-turtle'
                return response

        return wrapper

    def __register(self, handler, rdfizer):
        def decorator(f):
            self.__handlers[f.func_name] = handler
            self.__rdfizers[f.func_name] = rdfizer
            return f

        return decorator

    def register(self, path, handler, rdfizer):
        def decorator(f):
            for dec in [self.__execute, self.__register(handler, rdfizer), self.route(path)]:
                f = dec(f)
            return f

        return decorator
