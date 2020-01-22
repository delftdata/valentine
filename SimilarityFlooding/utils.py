import networkx as nx

class Node:

    '''
        Class for describing a node of a graph.
    '''

    def __init__(self, name, db):
        self.name = name
        self.db = db

    def __eq__(self, other):

        if isinstance(other, Node):
            return self.name == other.name and self.db == other.db

        return False

    def __hash__(self):
        return hash(self.name)


class NodePair:

    '''
        Class for describing a map pair in the propagation graph
    '''
    def __init__(self, node1, node2):
        self.node1 = node1
        self.node2 = node2

    def __eq__(self, other):

        if isinstance(other, NodePair):
            return (self.node1 == other.node1 and self.node2 == other.node2) or (self.node1 == other.node2 and self.node2 == other.node1)

    def __hash__(self):
        return hash(self.node1.name + self.node2.name)


def parseFile(dbname, schemaFile):

    '''
    Function to parse input file (in a very simple format for describing a schema)
    :param dbname: the name of the database
    :param schemaFile: the schema file describing the corresponding database
    :return: return a graph representation of the database as described in the paper
    '''
    G = nx.DiGraph()

    table = "Table"
    column = "Column"
    columntype = "ColumnType"

    tableNode = Node(table, dbname)
    columnNode = Node(column, dbname)
    coltypeNode = Node(columntype, dbname)
    G.add_node(tableNode)
    G.add_node(columnNode)
    G.add_node(coltypeNode)
    unique_id = 0
    with open(schemaFile, "r") as file:
        for line in file:
            toks = line.split(":")
            if len(toks) < 2:
                unique_id += 1
                parentNode = Node("NodeID" + str(unique_id), dbname)
                relationNode = Node(toks[0].rstrip(), dbname)
                G.add_node(parentNode)
                G.add_node(relationNode)
                G.add_edge(parentNode, relationNode, label = 'name')
                G.add_edge(parentNode, tableNode, label = 'type')
            else:
                unique_id+=1
                elementNode = Node("NodeID" + str(unique_id), dbname)
                G.add_node(elementNode)
                G.add_edge(elementNode, columnNode, label = 'type')
                G.add_edge(parentNode, elementNode, label = 'column')
                attributeNode = Node(toks[0].rstrip(), dbname)
                G.add_edge(elementNode, attributeNode, label = 'name')
                if G.has_node(Node(toks[1].rstrip(), dbname)):
                    G.add_edge(elementNode, [n for n in G.predecessors(Node(toks[1].rstrip(), dbname))][0], label = 'SQLtype')
                else:
                    previousid = unique_id
                    unique_id +=1
                    elementNode = Node("NodeID" + str(unique_id), dbname)
                    G.add_node(elementNode)
                    G.add_edge(elementNode, coltypeNode, label = 'type')
                    typenameNode = Node(toks[1].rstrip(), dbname)
                    G.add_node(typenameNode)
                    G.add_edge(elementNode, typenameNode, label = 'name')
                    G.add_edge(Node("NodeID" + str(previousid), dbname), elementNode, label = 'SQLtype')
    return G
