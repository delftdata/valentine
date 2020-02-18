from neo4j.v1 import GraphDatabase
from tqdm import tqdm

from api.apiutils import Relation
from knowledgerepr import fieldnetwork


class Neo4jExporter(object):
    def __init__(self,
                 host='localhost',
                 port=7687,
                 user="neo4j",
                 pwd="aurum"):
        self._server = f"bolt://{host}:{port}"
        self._user = user
        self._pwd = pwd

        self._driver = GraphDatabase.driver(self._server, auth=(user, pwd))

    def export(self, path_to_model):
        field_network = fieldnetwork.deserialize_network(path_to_model)

        # Create index to speed up MATCHes
        with self._driver.session() as session:
            session.run("CREATE INDEX ON :Node(nid)")

        for relation_label in Relation:

            # relation_hits is a generator. We could consume it to a list and then iterate over it,
            # but this would probably consume too much memory in most scenarios
            relation_hits = field_network.enumerate_relation(relation_label, as_str=False)
            for a, b in tqdm(relation_hits, desc=f'Storing {relation_label} relations to Neo4j', unit='relation'):
                with self._driver.session() as session:
                    # Step 1: add nodes
                    session.run(
                        "CREATE (n:Node {nid:$nid,db_name:$db_name,source:$source,field:$field,score:$score}) RETURN id(n)",
                        nid=a.nid, db_name=a.db_name, source=a.source_name, field=a.field_name, score=a.score)
                    session.run("CREATE (n:Node {nid:$nid,source:$source,field:$field,score:$score}) RETURN id(n)",
                                nid=b.nid, db_name=b.db_name, source=b.source_name, field=b.field_name, score=b.score)

                    session.run(
                        f"MATCH (a:Node),(b:Node)"
                        " WHERE a.nid=$nid_a AND b.nid=$nid_b "
                        "CREATE (a)-[r: {relation_label}]->(b) RETURN type(r)".format(
                            relation_label=str(relation_label).replace('Relation.', '')),
                        nid_a=a.nid, nid_b=b.nid)  # .single().value()
