import psycopg2 as db


class PGStore:

    def __init__(self, db_ip="localhost", db_port="5432", db_name=None, db_user=None, db_passwd=None):
        self.db_ip = db_ip
        self.db_port = db_port
        self.db_name = db_name
        self.db_user = db_user
        self.db_passwd = db_passwd
        self.con = self.connect()
        self.cur = self.con.cursor()

    def connect(self):
        connection_string = "postgresql://" +\
                             self.db_user + ":" + \
                             self.db_passwd + "@" + \
                             self.db_ip + ":" + \
                             self.db_port + "/" + \
                             self.db_name
        con = db.connect(connection_string)
        return con

    def init_schema(self):
        self.cur.execute("CREATE TABLE nodes (node_id integer PRIMARY KEY, "
                         "node_name varchar, "
                         "table_name varchar, "
                         "unique_ratio real);")
        self.cur.execute("CREATE TABLE edges (source_node_id integer, "
                         "target_node_id integer, "
                         "relation_type smallint, "
                         "weight real);")
        self.con.commit()

    def reset_cursor(self):
        self.cur.close()
        self.cur = self.con.cursor()

    def close_con(self):
        self.cur.close()
        self.con.close()

    def rollback(self):
        self.con.rollback()

    def commit(self):
        self.con.commit()

    """
    WRITE
    """

    def new_node(self, node_id=None, node_name=None, table_name=None, unique_ratio=None):
        self.cur.execute("insert into nodes (node_id, node_name, table_name, unique_ratio) "
                         "values (%s, %s, %s, %s)",
                         (node_id, node_name, table_name, unique_ratio))

    def new_edge(self, source_node_id=None, target_node_id=None, relation_type=None, weight=None):
        self.cur.execute("insert into edges (source_node_id, target_node_id, relation_type, weight) "
                         "values (%s, %s, %s, %s)",
                         (source_node_id, target_node_id, relation_type, weight))

    """
    READ
    """

    def connected_to(self, nid, relation_type):
        self.cur.execute("select target_node_id from edges where source_node_id = %s and relation_type = %s",
                         (nid, relation_type))
        results = []
        res = self.cur.fetchmany(size=10)
        while res:
            for el in res:
                results.append(el)
            res = self.cur.fetchmany(size=10)
        return results

    def paths(self, source_id, target_id, relation_type=1):
        self.cur.execute("WITH RECURSIVE transitive_closure (src, tgt, path_string) AS "
                         "(SELECT e.source_node_id, "
                         "e.target_node_id, e.source_node_id || %(dot_str)s || e.target_node_id || %(dot_str)s"
                         "AS path_string FROM edges e WHERE e.source_node_id = %(source_id)s"
                         "UNION "
                         "SELECT tc.src, e.target_node_id, tc.path_string || e.target_node_id || %(dot_str)s "
                         "AS path_string FROM edges "
                         "AS e JOIN transitive_closure AS tc ON e.source_node_id = tc.tgt "
                         "WHERE tc.path_string NOT LIKE %(perc_str)s || e.target_node_id || %(dotperc_str)s ) "
                         "SELECT * FROM transitive_closure tc WHERE tc.tgt = %(tid)s",
                         {'dot_str': '.',
                          'source_id': source_id,
                          'perc_str': '%',
                          'dotperc_str': '.%',
                          'tid': target_id})

        results = []
        res = self.cur.fetchmany(size=10)
        while res:
            for el in res:
                results.append(el)
            res = self.cur.fetchmany(size=10)
        return results

if __name__ == "__main__":
    print("pg store")

    store = PGStore("localhost", "5432", "test_py", "postgres", "admin")

    cons_to = store.connected_to(23, 1)
    for el in cons_to:
        print(str(el))

    import time
    stime = time.time()

    paths = store.paths(0, 8, 1)
    for el in paths:
        print(str(el))
    etime = time.time()
    print("path-query time: " + str(etime-stime))

    store.close_con()
    exit()

    import networkx as nx

    nodes = 100
    edge_probability = 0.5  # fairly populated graph
    random_g = nx.fast_gnp_random_graph(nodes, edge_probability)

    for src_id, tgt_id in random_g.edges():
        try:
            store.new_node(node_id=src_id, node_name=str(src_id), table_name="test", unique_ratio=0.9)
        except db.IntegrityError:
            store.rollback()
        try:
            store.new_node(node_id=tgt_id, node_name=str(tgt_id), table_name="test", unique_ratio=0.9)
        except db.IntegrityError:
            store.rollback()
        store.new_edge(source_node_id=src_id, target_node_id=tgt_id, relation_type=1, weight=0.55)
        store.commit()

    # paths = nx.all_shortest_paths(random_g, source=0, target=1)
    # for p in paths:
    #     print(str(p))

    store.close_con()
    print("Done!")

"""  TEST
with recursive r as (select source_node_id, target_node_id from edges where source_node_id = 0
union all
select edges.source_node_id, edges.target_node_id from edges inner join r on edges.source_node_id = r.target_node_id)
select source_node_id from r;
"""

""" WORKING VERSION
WITH RECURSIVE transitive_closure (src, tgt, path_string) AS
  (

   SELECT e.source_node_id, e.target_node_id,
   e.source_node_id || '.' || e.target_node_id || '.' AS path_string
   FROM edges e
   WHERE e.source_node_id = 0 --source_node

   UNION

   SELECT tc.src, e.target_node_id, tc.path_string || e.target_node_id || '.' AS path_string
   FROM edges AS e JOIN transitive_closure AS tc ON e.source_node_id = tc.tgt
   WHERE tc.path_string NOT LIKE '%' || e.target_node_id || '.%'
  )
  SELECT *
  FROM transitive_closure tc
  WHERE tc.tgt = 7;
"""

""" WORKING VERSION MAX-HOPS
WITH RECURSIVE transitive_closure (src, tgt, path_string, it) AS
  (

   SELECT e.source_node_id, e.target_node_id,
   e.source_node_id || '.' || e.target_node_id || '.' AS path_string, 0 as it
   FROM edges e
   WHERE e.source_node_id = 0 --source_node

   UNION

   SELECT tc.src, e.target_node_id, tc.path_string || e.target_node_id || '.' AS path_string, it + 1
   FROM edges AS e JOIN transitive_closure AS tc ON e.source_node_id = tc.tgt
   WHERE tc.path_string NOT LIKE '%' || e.target_node_id || '.%' AND it < 2
  )
  SELECT *
  FROM transitive_closure tc
  WHERE tc.tgt = 7;
"""