from anytree import RenderTree

from .linguistic_matching import normalization
from .schema_element_node import SchemaElementNode


class SchemaTree:
    def __init__(self, root):
        self.nodes = dict()  # k: node name v: SchemaElementNode object
        self.add_node(root)
        self.schema_name = root
        self.schema_tree = None

    def get_schema_tree(self):
        return RenderTree(self.nodes[self.schema_name])

    def get_node(self, node_name):
        return self.nodes[node_name]

    def add_node(self, table_name, table_guid=None, column_name=None, column_guid=None, data_type=None, parent=None):
        if parent:
            if data_type == "Table":
                self.nodes[table_name] = SchemaElementNode(table_name, parent=parent)
                self.nodes[table_name].tokens = normalization(table_name).tokens
                self.nodes[table_name].data_type = data_type
                self.nodes[table_name].add_category(data_type)
                self.nodes[table_name].add_long_name(parent.name, "", table_name, table_guid)
            else:
                self.nodes[column_name] = SchemaElementNode(column_name, parent=parent)
                self.nodes[column_name].tokens = normalization(column_name).tokens
                self.nodes[column_name].data_type = data_type
                self.nodes[column_name].add_category(data_type)
                self.nodes[column_name].add_long_name(parent.name, table_guid, column_name, column_guid)
        else:
            self.nodes[table_name] = SchemaElementNode(table_name)
            self.nodes[table_name].add_category("Database")

    def print_schema_tree(self):
        for pre, _, node in self.get_schema_tree():
            tree_str = u"%s%s" % (pre, node.name + str(node.categories))
            print(tree_str.ljust(8))

    def get_leaves(self):
        return self.get_node(self.schema_name).leaves

    def get_leaf_names(self):
        return tuple(map(lambda x: x.long_name, self.get_leaves()))

    @property
    def height(self):
        return self.root.height

    @property
    def root(self):
        return self.nodes[self.schema_name]
