from datasketch import MinHash
from nltk.corpus import stopwords

from algorithms.sem_prop.dataanalysis import nlp_utils as nlp


def retrieve_class_names(kr_handlers, num_perm=32):
    names = list()

    for kr_name, kr_handler in kr_handlers.items():
        all_classes = kr_handler.classes()
        for cl in all_classes:
            original_cl_name = cl
            cl = nlp.camelcase_to_snakecase(cl)
            cl = cl.replace('-', ' ')
            cl = cl.replace('_', ' ')
            cl = cl.lower()
            m = MinHash(num_perm=num_perm)
            for token in cl.split():
                if token not in stopwords.words('english'):
                    m.update(token.encode('utf8'))
            names.append(('class', (kr_name, original_cl_name), m))
    return names

