import pickle
import sys
import time

import ontospy
import rdflib

from algorithms.sem_prop.dataanalysis import nlp_utils as nlp
from algorithms.sem_prop.ontomatch.ss_utils import minhash

# We are serializing highly nested structures here...
sys.setrecursionlimit(50000)


class OntoHandler:

    def __init__(self):
        self.ontology_name = None
        self.o = None
        self.objectProperties = []
        self.class_hierarchy = []
        self.class_hierarchy_signatures = []
        self.map_classname_class = dict()

    def parse_ontology(self, file):
        """
        Preprocess ontology into library-specific format
        :param file:
        :return:
        """
        self.ontology_name = file
        ont = ontospy.Ontospy(file)
        self.o = ont
        self.objectProperties = self.o.all_properties  # cache this
        self.obtain_class_hierarchy_and_signatures()

    def obtain_class_hierarchy_and_signatures(self):
        self.class_hierarchy = self.__get_class_levels_hierarchy()  # preprocess this
        self.class_hierarchy_signatures = self.compute_classes_signatures()

    def store_ontology(self, path):
        """
        Serialize processed ontology to avoid pre-processing time
        :param ont:
        :param path:
        :return:
        """
        f = open(path, 'wb')
        pickle.dump(self.o, f)
        pickle.dump(self.class_hierarchy, f)
        pickle.dump(self.class_hierarchy_signatures, f)
        f.close()

    def load_ontology(self, path):
        """
        Load processed ontology
        :param path:
        :return:
        """
        f = open(path, 'rb')
        self.o = pickle.load(f)
        self.class_hierarchy = pickle.load(f)
        self.class_hierarchy_signatures = pickle.load(f)
        self.objectProperties = self.o.all_properties_object
        self.map_classname_class = dict()
        for c in self.o.all_classes:
            label = c.bestLabel().title()
            self.map_classname_class[label] = c
        f.close()
        #self.class_hierarchy = self.__get_class_levels_hierarchy()  # pre_load this

    def classes(self):
        """
        Return list of classes
        :param o:
        :return:
        """
        return [x.bestLabel().title() for x in self.o.all_classes]

    def class_and_descr(self):
        for x in self.o.all_classes:
            class_name = x.bestLabel().title()
            descr = x.bestDescription()
            yield (class_name, descr)

    def classes_id(self):
        """
        Return list of IDs
        :return:
        """
        return [x.id for x in self.o.all_classes]

    def name_of_class(self, class_id):
        """
        Returns name of a class given its id
        :param class_id:
        :return:
        """
        c = self.o.getClass(id=class_id)
        name = c.bestLabel().title()  # title to avoid rdflib types
        return name

    def id_of_class(self, class_name):
        """
        Returns class id given its name, or -1 if no class named that way
        :param class_name:
        :return:
        """
        c = self.o.getClass(match=class_name)[0]
        if len(c) > 0:
            cid = c.id
            return cid
        else:
            return -1

    def fake(self):
        return self.__get_class_levels_hierarchy()

    def get_class_from_name(self, class_name):
        if class_name in self.map_classname_class:
            return self.map_classname_class[class_name]
        else:
            return None

    def ancestors_of_class(self, c):
        """
        Ancestors of given class
        """
        class_from_name = self.get_class_from_name(c)
        if class_from_name is None:
            return []
        ancestors = [el for el in reversed(class_from_name.ancestors())]
        return ancestors

    def parents_of_class(self, c):
        """
        Ancestors of given class
        """
        class_from_name = self.get_class_from_name(c)
        if class_from_name is None:
            return []
        parents = [el for el in reversed(class_from_name.parents())]
        return parents

    def name_of_sequence(self, seq):
        seq_name = []
        for s in seq:
            name = s.bestLabel().title()
            seq_name.append(name)
        return seq_name

    def descendants_of_class(self, c):
        """
        Descendants of given class
        """
        return list(self.__get_descendants_of_class(c))

    def __get_ancestors_of_class(self, c):
        ancestors = set(c.parents())
        for parent in c.parents():
            ancestors |= self.__get_ancestors_of_class(parent)
        return ancestors

    def __get_descendants_of_class(self, c):
        descendants = set(c.children())
        for child in c.children():
            descendants |= self.__get_descendants_of_class(child)
        return descendants

    def get_properties_all_of(self, c):
        properties = self.o.getInferredPropertiesForClass(c)
        props = []
        for hierarchy_props in properties:
            for p in hierarchy_props.values():
                props.extend(p)
        return props

    def get_properties_only_of(self, c):
        properties = self.o.getInferredPropertiesForClass(c)
        props_dic = properties[0]
        props = list(props_dic.values())
        return props[0]

    def __get_class_levels_hierarchy(self):

        flatten = []

        for c in self.o.all_classes:
            if len(c.children()) > 0:
                el = (c.bestLabel().title(), [(ch.id, ch.bestLabel().title()) for ch in c.children()])
                flatten.append(el)
        return flatten

    def __get_class_levels_hierarchy2(self, element=None):
        if not element:  # then levels is also None, create a list
            levels = [(self.ontology_name, [(top.id, top.bestLabel().title())
                                            for top in self.o.toplayer])]  # name of top-level level is onto name
            for x in self.o.toplayer:
                print(str(len(levels)))
                levels.extend(self.__get_class_levels_hierarchy2(element=x))
            return levels

        if not element.children():
            return []

        levels = [(element.bestLabel().title(), [(child.id, child.bestLabel().title())
                                                 for child in element.children()])]  # name of parent
        for sub in element.children():
            levels.extend(self.__get_class_levels_hierarchy2(element=sub))
        return levels

    def class_levels_count(self):
        """
        Return the number of levels in the class hierarchy. This is equivalent to nodes in a tree.
        :return:
        """
        return len(self.class_hierarchy)

    def class_hierarchy_iterator(self):
        """
        Returns lists of classes that are at the same level of the hierarhcy, i.e., node in a tree
        :return:
        """
        for level in self.class_hierarchy:
            name = level[0]  # str with the name of the level (parent name)
            classes = level[1]  # [(id, class_name)]
            yield name, classes

    def compute_classes_signatures(self):
        """
        Return a minhash signature of the class-names per class hierarchy level
        :return:
        """
        st = time.time()
        mh_time = 0
        class_hierarchy_signatures = []
        for level_name, list_classes in self.class_hierarchy_iterator():
            if len(list_classes) < 10:
                continue  # filter out short classes
            ch = [el[1] for el in list_classes]
            smh = time.time()
            mh = minhash(ch)
            emh = time.time()
            mh_time += (emh - smh)
            chs = (level_name, mh)
            class_hierarchy_signatures.append(chs)
        et = time.time()
        total_time = et - st
        print("Total time signatures: " + str(total_time))
        print("Total time mh: " + str(mh_time))
        print("Ratio: " + str(mh_time/total_time))
        return class_hierarchy_signatures

    def get_classes_signatures(self, filter=None):
        if filter is None:
            return self.class_hierarchy_signatures
        else:
            return [el for el in self.class_hierarchy_signatures]

    # def parents_of_class(self, class_name, class_id=False):
    #     """
    #     Parents of given class
    #     :param class_name:
    #     :return:
    #     """
    #     if class_id:
    #         return self.o.getClass(id=class_name).parents()
    #     return self.o.getClass(match=class_name)[0].parents()

    def children_of_class(self, class_name, class_id=False):
        """
        Children of given class
        :param class_name:
        :return:
        """
        if class_id:
            return self.o.getClass(id=class_name).children()
        return self.o.getClass(match=class_name)[0].children()

    # def properties_all_of(self, class_name, class_id=False):
    #     """
    #     All properties associated to the given class (both datatype and object)
    #     :param class_name:
    #     :return:
    #     """
    #     if class_id:
    #         c = self.o.getClass(id=class_name)
    #     else:
    #         c = self.o.getClass(match=class_name)[0]
    #     return self.get_properties_all_of(c)
    #
    # def properties_only_of(self, class_name, class_id=False):
    #     """
    #     All properties associated to the given class (both datatype and object)
    #     :param class_name:
    #     :return:
    #     """
    #     if class_id:
    #         c = self.o.getClass(id=class_name)
    #     else:
    #         c = self.o.getClass(match=class_name)[0]
    #     return self.get_properties_only_of(c)

    def get_class_data_signatures(self):
        signatures = []
        for cid in self.classes_id():
            data = self.instances_of(cid, class_id=True)
            sig = minhash(data)
            class_name = self.name_of_class(cid)
            signatures.append((class_name, sig))
        return signatures

    def instances_of(self, class_name, class_id=False):
        """
        When data is available, retrieve all data for the given class
        :param c:
        :param p:
        :return:
        """
        if class_id:
            c = self.o.getClass(id=class_name)
        else:
            c = self.o.getClass(match=class_name)[0]
        clean_data = []
        data = c.instances()
        for d in data:
            if type(d) == rdflib.term.URIRef:
                d = d.title()
            clean_data.append(d)
        return clean_data

    def relations_of(self, class_name, class_id=False):
        """
        Return for the given class all properties that refere to other classes, i.e., they are relations
        :param c:
        :return:
        """
        if class_id:
            c = self.o.getClass(id=class_name)
        else:
            c = self.o.getClass(match=class_name)[0]
        property_for_class = self.o.getInferredPropertiesForClass(c)[:1]
        properties = []
        for dic in property_for_class:
            for k, v in dic.items():
                if len(v) > 0:
                    properties += v
        relations = []  # hosting (label, descr)
        for p in properties:
            if p in self.objectProperties:
                label = p.bestLabel()
                descr = p.bestDescription()
                relations.append((label, descr))
        return relations

    def bow_repr_of(self, class_name, class_id=False):
        """
        Retrieve a bag of words (bow) representation of the given class
        :param c:
        :return: (boolean, (class_name, bow)) if a bow can be built, or (boolean, str:reason) if not
        """
        if class_id:
            c = self.o.getClass(id=class_name)
        else:
            c = self.o.getClass(match=class_name)[0]
            if c is not None:
                c = c[0]
        if c is None:
            return False, "Class does not exist"  # means there's no

        label = c.bestLabel()
        descr = c.bestDescription()

        if descr is None or descr == "":
            return False, 'no descr here'  # we won't harness enough context...

        # Get class name, description -> bow, properties -> bow
        pnouns = nlp.get_proper_nouns(descr)
        nouns = nlp.get_nouns(descr)
        bow_descr = pnouns + nouns
        props = self.relations_of(class_name, class_id=class_id)
        bow_properties = []
        for prop_label, prop_descr, in props:
            tokens = nlp.tokenize_property(prop_label)
            bow_properties.extend(tokens)
        bow = bow_descr + bow_properties
        bow = nlp.curate_tokens(bow)
        ret = (label, bow)
        return True, ret


def parse_ontology(input_ontology_path, output_parsed_ontology_path):
    s = time.time()
    o = OntoHandler()
    o.parse_ontology(input_ontology_path)
    o.store_ontology(output_parsed_ontology_path)
    e = time.time()
    print("Parse ontology took: " + str(e - s))

if __name__ == '__main__':

    input = "merck_dlc.owl"
    output = "cache_onto/dlc.pkl"

    parse_ontology(input, output)

    exit()

    o = OntoHandler()

    o.load_ontology("cache_onto/dlc.pkl")

    total = 0
    nodesc = 0
    for c in o.o.classes:
        total += 1
        label = c.bestLabel()
        print(str(label))
        print(str(label.title()))
        descr = c.bestDescription()
        if descr == "":
            nodesc += 1

    for c in o.classes():
        print(str(c))

    print(str(nodesc) + "/" + str(total) + " no descr")
    # EFO output: 18604/19230 no descr
