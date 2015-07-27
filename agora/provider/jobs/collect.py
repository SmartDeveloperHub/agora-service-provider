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

from agora.client.agora import Agora, AGORA
from rdflib import RDF, RDFS
import logging
from rdflib import Literal
import time

__triple_patterns = {}
__plan_patterns = {}

log = logging.getLogger('agora.provider')


def collect(tp, *args):
    """
    Decorator to attach a collector function to a triple pattern
    :param tp:
    :param args:
    :return:
    """

    def decorator(f):
        add_triple_pattern(tp, f, args)

    return decorator


def add_triple_pattern(tp, collector, args):
    """0
    Manage the relations between triple patterns and collector functions
    :param tp:
    :param collector:
    :param args:
    :return:
    """
    tp_parts = [part.strip() for part in tp.strip().split(' ')]
    tp = ' '.join(tp_parts)
    if tp not in __triple_patterns.keys():
        __triple_patterns[tp] = set([])
    if collector is not None:
        __triple_patterns[tp].add((collector, args))


def __extract_pattern_nodes(graph):
    """
    Extract and bind the triple patterns contained in the search plan, so as to be able to identify
    to which pattern is associated each triple of the fragment.
    :return:
    """
    tp_nodes = graph.subjects(RDF.type, AGORA.TriplePattern)
    for tpn in tp_nodes:
        subject = list(graph.objects(tpn, AGORA.subject)).pop()
        predicate = list(graph.objects(tpn, AGORA.predicate)).pop()
        obj = list(graph.objects(tpn, AGORA.object)).pop()
        subject_str = list(graph.objects(subject, RDFS.label)).pop().toPython()
        predicate_str = graph.qname(predicate)
        if (obj, RDF.type, AGORA.Variable) in graph:
            object_str = list(graph.objects(obj, RDFS.label)).pop().toPython()
        else:
            object_str = list(graph.objects(obj, AGORA.value)).pop().toPython()
        __plan_patterns[tpn] = '{} {} {}'.format(subject_str, predicate_str, object_str)


def collect_fragment(event, agora_host):
    """
    Execute a search plan for the declared graph pattern and sends all obtained triples to the corresponding
    collector functions (config
    """
    agora = Agora(agora_host)
    graph_pattern = ""
    for tp in __triple_patterns:
        graph_pattern += '{} . '.format(tp)
    fragment, _, graph = agora.get_fragment_generator('{%s}' % graph_pattern, stop_event=event, workers=4)
    __extract_pattern_nodes(graph)
    log.info('querying { %s}' % graph_pattern)
    for (t, s, p, o) in fragment:
        collectors = __triple_patterns[str(__plan_patterns[t])]
        for c, args in collectors:
            log.debug('Sending triple {} {} {} to {}'.format(s.n3(graph.namespace_manager), graph.qname(p),
                                                             o.n3(graph.namespace_manager), c))
            c((s, p, o))
            if event.isSet():
                raise Exception('Abort collecting fragment')
            yield (c.func_name, (t, s, p, o))
        time.sleep(0.01)
