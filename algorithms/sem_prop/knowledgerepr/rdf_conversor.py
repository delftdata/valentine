import rdflib
from rdflib import Namespace

n = Namespace("http://ex.org/")

def parse_line(l):
    tokens = l.split(",")
    source = tokens[0]
    target = tokens[1]
    type = tokens[2]
    return source, target, type


def yield_triple(path):
    with open(path, 'r') as f:
        first = True
        for l in f:
            if first:  # skip header
                first = False
                continue
            source, target, type = parse_line(l)
            yield (source, target, type)


def create_rdf_graph_from_csv(csv_path):
    from rdflib import Graph
    g = Graph()
    c = 0
    for source, target, rtype in yield_triple(csv_path):
        source = source.rstrip().strip()
        target = target.rstrip().strip()
        rtype = rtype.rstrip().strip()
        c += 1
        if c % 25000 == 0:
            print(str(c))
        s = rdflib.term.URIRef("http://ex.org/" + source)
        o = rdflib.term.URIRef("http://ex.org/" + rtype)
        t = rdflib.term.URIRef("http://ex.org/" + target)
        g.add((s, o, t))
    g.serialize(destination="test.rdf", format='xml')


if __name__ == "__main__":
    print("RDF conversor")

    create_rdf_graph_from_csv("/Users/ra-mit/Downloads/graph-1.csv")

