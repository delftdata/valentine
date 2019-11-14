import itertools

from anytree import Node, RenderTree, PostOrderIter

from cupid.linguistic_matching import normalize
from cupid.tree_match import tree_match, get_matchings


class Cupid:
    def __init__(self):
        self.data = list()

    def add_schema(self, name, data_type):
        element = normalize(name)
        element.data_type = data_type
        element.add_category(data_type)
        schema = Node(element)
        self.data.append(schema)

    def get_schema(self, name):
        return next(filter(lambda d: d.name.initial_name == name, self.data))

    def add_table(self, schema_name, table_name, data_type):
        schema = next(filter(lambda d: d.name.initial_name == schema_name, self.data))
        element = normalize(table_name)
        element.data_type = data_type
        element.add_category(data_type)
        table = Node(element, parent=schema)

    def get_table(self, schema_name, table_name):
        schema = next(filter(lambda d: d.name.initial_name == schema_name, self.data))
        for child in schema.children:
            if child.name.initial_name == table_name:
                return child

    def add_columns_to_table(self, schema_name, table_name, column_data_pairs):
        table = self.get_table(schema_name, table_name)
        for column, data_type in column_data_pairs:
            schema_element = normalize(column)
            schema_element.add_category(data_type)
            schema_element.data_type = data_type
            node = Node(schema_element, parent=table)

    def print_data(self):
        def render_tree(schema):
            for pre, fill, node in RenderTree(schema):
                print("%s%s" % (pre, node.name.initial_name))

        print('Render trees ... ')
        for tree in self.data:
            render_tree(tree)
            print()


def example():
    employees = ['EmployeeID', 'FirstName', 'LastName', 'Title', 'EmailName', 'Extension', 'Workphone']
    et = ['EmployeeIdFk', 'TeritoryId']

    cupid = Cupid()
    cupid.add_schema('rdb_schema', 'string')

    cupid.add_table('rdb_schema', 'employee', 'string')
    cupid.add_columns_to_table('rdb_schema', 'employee', zip(employees, itertools.repeat("string")))

    cupid.add_table('rdb_schema', 'employee-territory', 'string')
    cupid.add_columns_to_table('rdb_schema', 'employee-territory', zip(et, itertools.repeat('str')))

    cupid.print_data()

    print('Computing matchings ... ')
    sims = tree_match(cupid.get_table('rdb_schema', 'employee'), cupid.get_table('rdb_schema', 'employee-territory'))
    print(get_matchings(sims))

    return cupid, sims

