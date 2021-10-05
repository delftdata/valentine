from anytree import NodeMixin

from .schema_element import SchemaElement


class SchemaElementNode(SchemaElement, NodeMixin):
    def __init__(self, name, parent=None, children=None):
        super(SchemaElementNode, self).__init__(name=name)
        self.name = name
        self.parent = parent
        if children:
            self.children = children

    def get_leaf_names(self):
        return tuple(map(lambda x: x.long_name, self.leaves))
