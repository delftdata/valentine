import nltk
import re
from nltk.corpus import stopwords

english = stopwords.words('english')


def pos_tag_text(text):
    tagged = nltk.tag.pos_tag(text.split())
    return tagged


def get_word_with_pos(text, type):
    tagged = pos_tag_text(text)
    words = [w for w, pos in tagged if pos == type]
    return words


def get_nouns(text):
    nouns = get_word_with_pos(text, 'NN')
    return nouns


def get_proper_nouns(text):
    pnouns = get_word_with_pos(text, 'NNP')
    return pnouns


def camelcase_to_snakecase(term):
    tmp = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', term)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', tmp).lower()


def tokenize_property(prop):
    snake = camelcase_to_snakecase(prop)
    snake = snake.replace('_', ' ')
    snake = snake.replace('-', ' ')
    tokens = snake.split(' ')
    return tokens


def curate_tokens(tokens):
    tokens = [w.lower() for w in tokens if len(w) > 1 and w not in english]
    tokens = list(set(tokens))
    return tokens


def curate_string(string):
    snake = camelcase_to_snakecase(string)
    snake = snake.replace('_', ' ')
    snake = snake.replace('-', ' ')
    snake.lower()
    return snake

if __name__ == "__main__":
    print("NLP Utils")
