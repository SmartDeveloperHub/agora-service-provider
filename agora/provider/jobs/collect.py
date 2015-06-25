__author__ = 'fernando'

from agora.client.agora import Agora, AGORA
from rdflib import RDF, RDFS

agora = Agora('http://localhost:5000')

__triple_patterns = {}
__plan_patterns = {}

def add_triple_pattern(tp, collector):
    tp_parts = [part.strip() for part in tp.strip().split(' ')]
    tp = ' '.join(tp_parts)
    if tp not in __triple_patterns.keys():
        __triple_patterns[tp] = set([])
    if collector is not None:
        __triple_patterns[tp].add(collector)


def __extract_pattern_nodes(graph):
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


def collect_fragment():
    graph_pattern = ""
    for tp in __triple_patterns:
        graph_pattern += '{} . '.format(tp)
    fragment, _, graph = agora.get_fragment_generator('{%s}' % graph_pattern)
    __extract_pattern_nodes(graph)
    print 'querying { %s}' % graph_pattern
    # i = 0
    for (t, s, p, o) in fragment:
        collectors = __triple_patterns[str(__plan_patterns[t])]
        for c in collectors:
            c(__plan_patterns[t], (s, p, o))
        # i += 1
        # if i == 100:
        #     break
