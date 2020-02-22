import itertools

from algorithms.sem_prop.modelstore.elasticstore import KWType

from algorithms.sem_prop.api.apiutils import compute_field_id as id_from
from algorithms.sem_prop.api.apiutils import Operation
from algorithms.sem_prop.api.apiutils import OP
from algorithms.sem_prop.api.apiutils import Relation
from algorithms.sem_prop.api.apiutils import DRS
from algorithms.sem_prop.api.apiutils import DRSMode
from algorithms.sem_prop.api.apiutils import Hit
from algorithms.sem_prop.api.annotation import MDClass
from algorithms.sem_prop.api.annotation import MDRelation
from algorithms.sem_prop.api.annotation import MRS


class Algebra:

    def __init__(self, network, store_client):
        self._network = network
        self._store_client = store_client
        self.helper = Helper(network=network, store_client=store_client)

    """
    Basic API
    """

    def search(self, kw: str, kw_type: KWType, max_results=10) -> DRS:
        """
        Performs a keyword search over the contents of the data.
        Scope specifies where elasticsearch should be looking for matches.
        i.e. table titles (SOURCE), columns (FIELD), or comment (SOURCE)

        :param kw: the keyword to serch
        :param kw_type: the context type on which to search
        :param max_results: maximum number of results to return
        :return: returns a DRS
        """

        hits = self._store_client.search_keywords(
            keywords=kw, elasticfieldname=kw_type, max_hits=max_results)

        # materialize generator
        drs = DRS([x for x in hits], Operation(OP.KW_LOOKUP, params=[kw]))
        return drs

    def exact_search(self, kw: str, kw_type: KWType, max_results=10):
        """
        See 'search'. This only returns exact matches.
        """

        hits = self._store_client.exact_search_keywords(
            keywords=kw, elasticfieldname=kw_type, max_hits=max_results)

        # materialize generator
        drs = DRS([x for x in hits], Operation(OP.KW_LOOKUP, params=[kw]))
        return drs

    def search_content(self, kw: str, max_results=10) -> DRS:
        return self.search(kw, kw_type=KWType.KW_CONTENT, max_results=max_results)

    def search_attribute(self, kw: str, max_results=10) -> DRS:
        return self.search(kw, kw_type=KWType.KW_SCHEMA, max_results=max_results)

    def search_exact_attribute(self, kw: str, max_results=10) -> DRS:
        return self.exact_search(kw, kw_type=KWType.KW_SCHEMA, max_results=max_results)

    def search_table(self, kw: str, max_results=10) -> DRS:
        return self.search(kw, kw_type=KWType.KW_TABLE, max_results=max_results)

    def suggest_schema(self, kw: str, max_results=5):
        return self._store_client.suggest_schema(kw, max_hits=max_results)

    def __neighbor_search(self,
                        input_data,
                        relation: Relation):
        """
        Given an nid, node, hit or DRS, finds neighbors with specified
        relation.
        :param nid, node tuple, Hit, or DRS:
        """
        # convert whatever input to a DRS
        i_drs = self._general_to_drs(input_data)

        # prepare an output DRS
        o_drs = DRS([], Operation(OP.NONE))
        o_drs = o_drs.absorb_provenance(i_drs)

        # get all of the table Hits in a DRS, if necessary.
        if i_drs.mode == DRSMode.TABLE:
            self._general_to_field_drs(i_drs)

        # Check neighbors
        if not relation.from_metadata():
            for h in i_drs:
                hits_drs = self._network.neighbors_id(h, relation)
                o_drs = o_drs.absorb(hits_drs)
        else:
            md_relation = self._relation_to_mdrelation(relation)
            for h in i_drs:
                neighbors = self.md_search(h, md_relation)
                hits_drs = self._network.md_neighbors_id(h, neighbors, relation)
                o_drs = o_drs.absorb(hits_drs)
        return o_drs

    def content_similar_to(self, general_input):
        return self.__neighbor_search(input_data=general_input, relation=Relation.CONTENT_SIM)

    def schema_similar_to(self, general_input):
        return self.__neighbor_search(input_data=general_input, relation=Relation.SCHEMA_SIM)

    def pkfk_of(self, general_input):
        return self.__neighbor_search(input_data=general_input, relation=Relation.PKFK)

    """
    TC API
    """

    def paths(self, drs_a: DRS, drs_b: DRS, relation=Relation.PKFK, max_hops=2, lean_search=False) -> DRS:
        """
        Is there a transitive relationship between any element in a with any
        element in b?
        This function finds the answer constrained on the primitive
        (singular for now) that is passed as a parameter.
        If b is not passed, assumes the user is searching for paths between
        elements in a.
        :param a: DRS
        :param b: DRS
        :param Relation: Relation
        :return:
        """
        # create b if it wasn't passed in.
        drs_a = self._general_to_drs(drs_a)
        drs_b = self._general_to_drs(drs_b)

        self._assert_same_mode(drs_a, drs_b)

        # absorb the provenance of both a and b
        o_drs = DRS([], Operation(OP.NONE))
        o_drs.absorb_provenance(drs_a)
        if drs_b != drs_a:
            o_drs.absorb_provenance(drs_b)

        for h1, h2 in itertools.product(drs_a, drs_b):

            # there are different network operations for table and field mode
            res_drs = None
            if drs_a.mode == DRSMode.FIELDS:
                res_drs = self._network.find_path_hit(
                    h1, h2, relation, max_hops=max_hops)
            else:
                res_drs = self._network.find_path_table(
                    h1, h2, relation, self, max_hops=max_hops, lean_search=lean_search)

            o_drs = o_drs.absorb(res_drs)

        return o_drs

    def __traverse(self, a: DRS, primitive, max_hops=2) -> DRS:
        """
        Conduct a breadth first search of nodes matching a primitive, starting
        with an initial DRS.
        :param a: a nid, node, tuple, or DRS
        :param primitive: The element to search
        :max_hops: maximum number of rounds on the graph
        """
        a = self._general_to_drs(a)

        o_drs = DRS([], Operation(OP.NONE))

        if a.mode == DRSMode.TABLE:
            raise ValueError(
                'input mode DRSMode.TABLE not supported')

        fringe = a
        o_drs.absorb_provenance(a)
        while max_hops > 0:
            max_hops = max_hops - 1
            for h in fringe:
                hits_drs = self._network.neighbors_id(h, primitive)
                o_drs = self.union(o_drs, hits_drs)
            fringe = o_drs  # grow the initial input
        return o_drs

    """
    Combiner API
    """

    def intersection(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are both in a and b
        :param a: an iterable object
        :param b: another iterable object
        :return: the intersection of the two provided iterable objects
        """
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        self._assert_same_mode(a, b)

        o_drs = a.intersection(b)
        return o_drs

    def union(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are in either a or b
        :param a: an iterable object
        :param b: another iterable object
        :return: the union of the two provided iterable objects
        """
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        self._assert_same_mode(a, b)

        o_drs = a.union(b)
        return o_drs

    def difference(self, a: DRS, b: DRS) -> DRS:
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        """
        Returns elements that are in either a or b
        :param a: an iterable object
        :param b: another iterable object
        :return: the union of the two provided iterable objects
        """
        a = self._general_to_drs(a)
        b = self._general_to_drs(b)
        self._assert_same_mode(a, b)

        o_drs = a.set_difference(b)
        return o_drs

    """
    Helper Functions
    """

    def make_drs(self, general_input):
        """
        Makes a DRS from general_input.
        general_input can include an array of strings, Hits, DRS's, etc,
        or just a single DRS.
        """
        try:

            # If this is a list of inputs, condense it into a single drs
            if isinstance(general_input, list):
                general_input = [
                    self._general_to_drs(x) for x in general_input]

                combined_drs = DRS([], Operation(OP.NONE))
                for drs in general_input:
                    combined_drs = self.union(combined_drs, drs)
                general_input = combined_drs

            # else, just convert it to a DRS
            o_drs = self._general_to_drs(general_input)
            return o_drs
        except:
            msg = (
                '--- Error ---' +
                '\nThis function returns domain result set from the ' +
                'supplied input' +
                '\nusage:\n\tmake_drs( table name/hit id | [table name/hit ' +
                'id, drs/hit/string/int] )' +
                '\ne.g.:\n\tmake_drs(1600820766)')
            print(msg)

    def _drs_from_table_hit_lean_no_provenance(self, hit: Hit) -> DRS:
        # TODO: migrated from old ddapi as there's no good swap
        table = hit.source_name
        hits = self._network.get_hits_from_table(table)
        drs = DRS([x for x in hits], Operation(OP.TABLE, params=[hit]), lean_drs=True)
        return drs

    def drs_from_table_hit(self, hit: Hit) -> DRS:
        # TODO: migrated from old ddapi as there's no good swap
        table = hit.source_name
        hits = self._network.get_hits_from_table(table)
        drs = DRS([x for x in hits], Operation(OP.TABLE, params=[hit]))
        return drs

    def _general_to_drs(self, general_input) -> DRS:
        """
        Given an nid, node, hit, or DRS and convert it to a DRS.
        :param nid: int
        :param node: (db_name, source_name, field_name)
        :param hit: Hit
        :param DRS: DRS
        :return: DRS
        """
        # test for DRS initially for speed
        if isinstance(general_input, DRS):
            return general_input

        if general_input is None:
            general_input = DRS(data=[], operation=Operation(OP.NONE))

        # Test for ints or strings that represent integers
        if self._represents_int(general_input):
            general_input = self._nid_to_hit(general_input)

        # Test for strings that represent tables
        if isinstance(general_input, str):
            hits = self._network.get_hits_from_table(general_input)
            general_input = DRS([x for x in hits], Operation(OP.ORIGIN))

        # Test for tuples that are not Hits
        if (isinstance(general_input, tuple) and
                not isinstance(general_input, Hit)):
            general_input = self._node_to_hit(general_input)

        # Test for Hits
        if isinstance(general_input, Hit):
            field = general_input.field_name
            if field == '' or field is None:
                # If the Hit's field is not defined, it is in table mode
                # and all Hits from the table need to be found
                general_input = self._hit_to_drs(
                    general_input, table_mode=True)
            else:
                general_input = self._hit_to_drs(general_input)
        if isinstance(general_input, DRS):
            return general_input

        raise ValueError(
            'Input is not None, an integer, field tuple, Hit, or DRS')

    def _nid_to_hit(self, nid: int) -> Hit:
        """
        Given a node id, convert it to a Hit
        :param nid: int or string
        :return: DRS
        """
        nid = str(nid)
        score = 0.0
        nid, db, source, field = self._network.get_info_for([nid])[0]
        hit = Hit(nid, db, source, field, score)
        return hit

    def _node_to_hit(self, node: (str, str, str)) -> Hit:
        """
        Given a field and source name, it returns a Hit with its representation
        :param node: a tuple with the name of the field,
            (db_name, source_name, field_name)
        :return: Hit
        """
        db, source, field = node
        nid = id_from(db, source, field)
        hit = Hit(nid, db, source, field, 0)
        return hit

    def _hit_to_drs(self, hit: Hit, table_mode=False) -> DRS:
        """
        Given a Hit, return a DRS. If in table mode, the resulting DRS will
        contain Hits representing that table.
        :param hit: Hit
        :param table_mode: if the Hit represents an entire table
        :return: DRS
        """
        drs = None
        if table_mode:
            table = hit.source_name
            hits = self._network.get_hits_from_table(table)
            drs = DRS([x for x in hits], Operation(OP.TABLE, params=[hit]))
            drs.set_table_mode()
        else:
            drs = DRS([hit], Operation(OP.ORIGIN))

        return drs

    def _general_to_field_drs(self, general_input):
        drs = self._general_to_drs(general_input)

        drs.set_fields_mode()
        for h in drs:
            fields_table = self._hit_to_drs(h, table_mode=True)
            drs = drs.absorb(fields_table)

        return drs

    def _mdclass_to_str(self, md_class: MDClass):
        ref_table = {
            MDClass.WARNING: "warning",
            MDClass.INSIGHT: "insight",
            MDClass.QUESTION: "question"
        }
        return ref_table[md_class]

    def _mdrelation_to_str(self, md_relation: MDRelation):
        """
        :return: (str, nid_is_source)
        """
        ref_table = {
            MDRelation.MEANS_SAME_AS: ("same", True),
            MDRelation.MEANS_DIFF_FROM: ("different", True),
            MDRelation.IS_SUBCLASS_OF: ("subclass", True),
            MDRelation.IS_SUPERCLASS_OF: ("subclass", False),
            MDRelation.IS_MEMBER_OF: ("member", True),
            MDRelation.IS_CONTAINER_OF: ("member", False)
        }
        return ref_table[md_relation]

    def _relation_to_mdrelation(self, relation):
        if relation == Relation.MEANS_SAME:
            return MDRelation.MEANS_SAME_AS
        if relation == Relation.MEANS_DIFF:
            return MDRelation.MEANS_DIFF_FROM
        if relation == Relation.SUBCLASS:
            return MDRelation.IS_SUBCLASS_OF
        if relation == Relation.SUPERCLASS:
            return MDRelation.IS_SUPERCLASS_OF
        if relation == Relation.MEMBER:
            return MDRelation.IS_MEMBER_OF
        if relation == Relation.CONTAINER:
            return MDRelation.IS_CONTAINER_OF

    def _assert_same_mode(self, a: DRS, b: DRS) -> None:
        error_text = ("Input parameters are not in the same mode ",
                      "(fields, table)")
        assert a.mode == b.mode, error_text

    def _represents_int(self, string: str) -> bool:
        try:
            int(string)
            return True
        except:
            return False

    """
    Metadata API
    """

    # Hide these for the time-being

    def __annotate(self, author: str, text: str, md_class: MDClass,
                 general_source, ref={"general_target": None, "type": None}) -> MRS:
        """
        Create a new annotation in the elasticsearch graph.
        :param author: identifiable name of user or process
        :param text: free text description
        :param md_class: MDClass
        :param general_source: nid, node tuple, Hit, or DRS
        :param ref: (optional) {
            "general_target": nid, node tuple, Hit, or DRS,
            "type": MDRelation
        }
        :return: MRS of the new metadata
        """
        source = self._general_to_drs(general_source)
        target = self._general_to_drs(ref["general_target"])

        if source.mode != DRSMode.FIELDS or target.mode != DRSMode.FIELDS:
            raise ValueError("source and targets must be columns")

        md_class = self._mdclass_to_str(md_class)
        md_hits = []

        # non-relational metadata
        if ref["type"] is None:
            for hit_source in source:
                res = self._store_client.add_annotation(
                    author=author,
                    text=text,
                    md_class=md_class,
                    source=hit_source.nid)
                md_hits.append(res)
            return MRS(md_hits)

        # relational metadata
        md_relation, nid_is_source = self._mdrelation_to_str(ref["type"])
        if not nid_is_source:
            source, target = target, source

        for hit_source in source:
            for hit_target in target:
                res = self._store_client.add_annotation(
                    author=author,
                    text=text,
                    md_class=md_class,
                    source=hit_source.nid,
                    target={"id": hit_target.nid, "type": md_relation})
                md_hits.append(res)
            return MRS(md_hits)

    def __add_comments(self, author: str, comments: list, md_id: str) -> MRS:
        """
        Add comments to the annotation with the given md_id.
        :param author: identifiable name of user or process
        :param comments: list of free text comments
        :param md_id: metadata id
        """
        md_comments = []
        for comment in comments:
            res = self._store_client.add_comment(
                author=author, text=comment, md_id=md_id)
            md_comments.append(res)
        return MRS(md_comments)

    def __add_tags(self, author: str, tags: list, md_id: str):
        """
        Add tags/keywords to metadata with the given md_id.
        :param md_id: metadata id
        :param tags: a list of tags to add
        """
        return self._store_client.add_tags(author, tags, md_id)

    def __md_search(self, general_input=None,
                  relation: MDRelation = None) -> MRS:
        """
        Searches for metadata that reference the nodes in the general
        input. If a relation is given, searches for metadata that mention the
        nodes as the source of the relation. If no parameters are given,
        searches for all metadata.
        :param general_input: nid, node tuple, Hit, or DRS
        :param relation: an MDRelation
        """
        # return all metadata
        if general_input is None:
            return MRS([x for x in self._store_client.get_metadata()])

        drs_nodes = self._general_to_drs(general_input)
        if drs_nodes.mode != DRSMode.FIELDS:
            raise ValueError("general_input must be columns")

        # return metadata that reference the input
        if relation is None:
            md_hits = []
            for node in drs_nodes:
                md_hits.extend(self._store_client.get_metadata(nid=node.nid))
            return MRS(md_hits)

        # return metadata that reference the input with the given relation
        md_hits = []
        store_relation, nid_is_source = self._mdrelation_to_str(relation)
        for node in drs_nodes:
            md_hits.extend(self._store_client.get_metadata(nid=node.nid,
                                                           relation=store_relation, nid_is_source=nid_is_source))
        return MRS(md_hits)

    def __md_keyword_search(self, kw: str, max_results=10) -> MRS:
        """
        Performs a keyword search over metadata annotations and comments.
        :param kw: the keyword to search
        :param max_results: maximum number of results to return
        :return: returns a MRS
        """
        hits = self._store_client.search_keywords_md(
            keywords=kw, max_hits=max_results)

        mrs = MRS([x for x in hits])
        return mrs


class Helper:

    def __init__(self, network, store_client):
        self._network = network
        self._store_client = store_client

    def reverse_lookup(self, nid) -> [str]:
        info = self._network.get_info_for([nid])
        return info

    def get_path_nid(self, nid) -> str:
        path_str = self._store_client.get_path_of(nid)
        return path_str

    def help(self):
        """
        Prints general help information, or specific usage information of a function if provided
        :param function: an optional function
        """
        from IPython.display import Markdown, display

        def print_md(string):
            display(Markdown(string))

        # Check whether the request is for some specific function
        #if function is not None:
        #    print_md(self.function.__doc__)
        # If not then offer the general help menu
        #else:
        print_md("### Help Menu")
        print_md("You can use the system through an **API** object. API objects are returned"
                 "by the *init_system* function, so you can get one by doing:")
        print_md("***your_api_object = init_system('path_to_stored_model')***")
        print_md("Once you have access to an API object there are a few concepts that are useful "
                 "to use the API. **content** refers to actual values of a given field. For "
                 "example, if you have a table with an attribute called __Name__ and values *Olu, Mike, Sam*, content "
                 "refers to the actual values, e.g. Mike, Sam, Olu.")
        print_md("**schema** refers to the name of a given field. In the previous example, schema refers to the word"
                 "__Name__ as that's how the field is called.")
        print_md("Finally, **entity** refers to the *semantic type* of the content. This is in experimental state. For "
                 "the previous example it would return *'person'* as that's what those names refer to.")
        print_md("Certain functions require a *field* as input. In general a field is specified by the source name ("
                 "e.g. table name) and the field name (e.g. attribute name). For example, if we are interested in "
                 "finding content similar to the one of the attribute *year* in the table *Employee* we can provide "
                 "the field in the following way:")
        print(
            "field = ('Employee', 'year') # field = [<source_name>, <field_name>)")


class API(Algebra):
    def __init__(self, *args, **kwargs):
        # print(str(type(API)))
        # print(str(type(self)))
        super(API, self).__init__(*args, **kwargs)


if __name__ == '__main__':
    print("Aurum API")
