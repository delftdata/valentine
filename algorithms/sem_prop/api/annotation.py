from collections import namedtuple
from enum import Enum

BaseMDHit = namedtuple(
    'MDHit', 'id, author, md_class, text, source, target, relation')
BaseMDComment = namedtuple('MDComment', 'id, author, text, ref_id')


class MDClass(Enum):
    WARNING = 0
    INSIGHT = 1
    QUESTION = 2


class MDRelation(Enum):
    MEANS_SAME_AS = 0
    MEANS_DIFF_FROM = 1
    IS_SUBCLASS_OF = 2
    IS_SUPERCLASS_OF = 3
    IS_MEMBER_OF = 4
    IS_CONTAINER_OF = 5


class MDHit(BaseMDHit):

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, MDHit):
            return self.id == other.id
        elif isinstance(other, str):
            return self.id == other
        return False

    def __repr__(self):
        if self.target is None:
            relation = "{}".format(self.source)
        else:
            relation = "{} {} {}".format(
                self.source, self.relation, self.target)
        return "ID: {0:20} RELATION: {1:30} TEXT: {2}".format(
            self.id, relation, self.text)

    def __str__(self):
        return self.__repr__()


class MDComment(BaseMDComment):

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, MDComment):
            return self.id == other.id
        elif isinstance(other, str):
            return self.id == other
        return False

    def __repr__(self):
        return "ID: {0:20} REF_ID: {1:32} TEXT: {2}".format(
            self.id, self.ref_id, self.text)

    def __str__(self):
        return self.__repr__()


class MRS():

    def __init__(self, data):
        self._data = data
        self._idx = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._idx < len(self._data):
            self._idx += 1
            return self._data[self._idx - 1]
        else:
            self._idx = 0
            raise StopIteration

    def __repr__(self):
        return str.join("\n", map(str, self._data))

    def __str__(self):
        return self.__repr__()

    @property
    def data(self):
        return self._data

    def set_data(self, data):
        self._data = list(data)
        self._idx = 0
        return self

    def size(self):
        return len(self.data)
