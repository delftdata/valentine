import itertools

from anytree import Node, RenderTree

from cupid.linguistic_matching import normalization
from cupid.tree_match import tree_match, recompute_wsim, mapping_generation_leaves, mapping_generation_non_leaves


# TODO: make the code compatible for multiple categories
def create_cupid_element(data_type, element_name, source_name, category):
    element = normalization(element_name)
    element.data_type = data_type
    element.add_category(category)
    element.add_long_name(source_name, element_name)

    return element


class Cupid:
    def __init__(self):
        self.data = list()
        self.categories = dict()

    def add_data(self, schema_name, table_name, column_data_pairs, schema_type="none", table_type="none"):
        schema_node = self.get_schema_by_name(schema_name)
        if not schema_node:
            self.add_schema(schema_name, schema_type)
        self.add_table(schema_name, table_name, table_type)
        self.add_columns_to_table(schema_name, table_name, column_data_pairs)

    def add_schema(self, name, data_type="none"):
        element = create_cupid_element(data_type, name, name, data_type)
        schema = Node(element)
        self.data.append(schema)
        self.categories[name] = dict()

    def get_schema_by_name(self, name):
        nodes = list(filter(lambda d: d.name.initial_name == name, self.data))
        if len(nodes) > 0:
            return nodes[0]
        else:
            return None

    def get_schema_by_index(self, index):
        return self.data[index]

    def add_table(self, schema_name, table_name, data_type="string"):
        schema = self.get_schema_by_name(schema_name)
        if not schema:
            print("Please add a schema first")
            return
        element = create_cupid_element(data_type, table_name, table_name, data_type)
        table = Node(element, parent=schema)

    def get_table(self, schema_name, table_name):
        schema = self.get_schema_by_name(schema_name)
        if not schema:
            print("Please add a schema first")
            return
        for child in schema.children:
            if child.name.initial_name == table_name:
                return child
        return None

    def get_all_tables(self, schema_name):
        schema = next(filter(lambda d: d.name.initial_name == schema_name, self.data))
        return schema.children

    def add_columns_to_table(self, schema_name, table_name, column_data_pairs):
        table = self.get_table(schema_name, table_name)
        if not table:
            print("Please add a table first")
            return
        for column, data_type in column_data_pairs:
            element = create_cupid_element(data_type, column, table_name, data_type)
            node = Node(element, parent=table)

            if data_type not in self.categories[schema_name]:
                self.categories[schema_name][data_type] = list()
            self.categories[schema_name][data_type].append(element)

    def get_categories(self):
        return self.categories

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
    cupid.add_data('rdb_schema1', 'employee', zip(employees, itertools.repeat("string")))
    cupid.add_data('rdb_schema2', 'employee-territory', zip(et, itertools.repeat('str')))
    cupid.print_data()

    print('Computing matchings ... ')
    source_tree = cupid.get_schema_by_name('rdb_schema1')
    target_tree = cupid.get_schema_by_name('rdb_schema2')
    sims = tree_match(source_tree, target_tree, cupid.get_categories())
    new_sims = recompute_wsim(source_tree, target_tree, sims)

    print("Leaf matchings:\n {}".format(mapping_generation_leaves(source_tree, target_tree, sims)))
    # print("Non-leaf matchings:\n {}".format(mapping_generation_non_leaves(source_tree, target_tree, new_sims)))
