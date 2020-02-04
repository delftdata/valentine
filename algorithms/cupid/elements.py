from collections import namedtuple
from enum import Enum


class SchemaElement:
    def __init__(self, name):
        # an element can belong to multiple categories
        self.categories = list()
        self.data_type = None
        self.tokens = list()
        self.initial_name = name
        self.long_name = None

    def add_long_name(self, table_name, name):
        self.long_name = (table_name, name)

    def add_category(self, category):
        self.categories.append(category)

    def add_token(self, token):
        if type(token) is Token:
            self.tokens.append(token)
        else:
            print("Incorrect token type. The type should be 'Token'")

    def get_tokens_data(self, tokens=None):
        if tokens is None:
            return list(map(lambda t: t.data, self.tokens))
        else:
            return list(map(lambda t: t.data, tokens))

    def get_tokens_data_and_type(self, tokens=None):
        if tokens is None:
            return list(map(lambda t: (t.data, t.token_type), self.tokens))
        else:
            return list(map(lambda t: (t.data, t.token_type), tokens))

    def sort_by_token_type(self):
        return sorted(self.tokens, key=lambda token: token.token_type.token_name)

    def get_tokens_by_token_type(self, token_type):
        sorted_tokens = self.sort_by_token_type()
        return list(filter(lambda t: t.token_type == token_type, sorted_tokens))


class Token:
    def __init__(self):
        self.ignore = False
        self.data = None
        self.token_type = None

    def add_data(self, data):
        self.data = data
        return self


TokenType = namedtuple('TokenType', ['token_name', 'weight'])


class TokenTypes(Enum):
    SYMBOLS = TokenType('symbols', 0)
    NUMBER = TokenType('number', 0.1)
    COMMON_WORDS = TokenType('common words', 0.1)
    CONTENT = TokenType('content', 0.8)

    @property
    def weight(self):
        return self.value.weight

    @property
    def token_name(self):
        return self.value.token_name
