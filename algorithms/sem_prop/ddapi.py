from algorithms.sem_prop.api.apiutils import DRS
from algorithms.sem_prop.api.apiutils import DRSMode
from algorithms.sem_prop.api.apiutils import Hit
from algorithms.sem_prop.api.apiutils import OP
from algorithms.sem_prop.api.apiutils import Operation
from algorithms.sem_prop.api.apiutils import Relation
from algorithms.sem_prop.api.apiutils import compute_field_id as id_from
from algorithms.sem_prop.modelstore.elasticstore import KWType
from algorithms.sem_prop.modelstore.elasticstore import StoreHandler

store_client = None


class DDAPI:

    __network = None

    def __init__(self, network):
        self.__network = network

    """
    Seed API
    """

    def drs_from_raw_field(self, field: (str, str, str)) -> DRS:
        """
        Given a field and source name, it returns a DRS with its representation
        :param field: a tuple with the name of the field, (db_name, source_name, field_name)
        :return: a DRS with the source-field internal representation
        """
        db, source, field = field
        nid = id_from(db, source, field)
        h = Hit(nid, db, source, field, 0)
        return self.drs_from_hit(h)

    def drs_from_hit(self, hit: Hit) -> DRS:
        drs = DRS([hit], Operation(OP.ORIGIN))
        return drs

    def drs_from_hits(self, hits: [Hit]) -> DRS:
        drs = DRS(hits, Operation(OP.ORIGIN))
        return drs

    def drs_from_table(self, source: str) -> DRS:
        """
        Given a source, it retrieves all fields of the source and returns them
        in the internal representation
        :param source: string with the name of the table
        :return: a DRS with the source-field internal representation
        """
        hits = self.__network.get_hits_from_table(source)
        drs = DRS([x for x in hits], Operation(OP.ORIGIN))
        return drs

    def drs_from_table_hit(self, hit: Hit) -> DRS:
        table = hit.source_name
        hits = self.__network.get_hits_from_table(table)
        drs = DRS([x for x in hits], Operation(OP.TABLE, params=[hit]))
        return drs

    def drs_expand_to_table(self, drs: DRS) -> DRS:
        o_drs = DRS([], Operation(OP.NONE))
        for h in drs:
            table = h.source_name
            hits = self.__network.get_hits_from_table(table)
            drs = DRS([x for x in hits], Operation(OP.TABLE, params=[h]))
            o_drs.absorb(drs)
        return o_drs

    def reverse_lookup(self, nid) -> [str]:
        info = self.__network.get_info_for([nid])
        return info

    """
    View API
    """

    def fields(self, drs: DRS) -> DRS:
        """
        Given a DRS, it configures it to field view (default)
        :param drs: the DRS to configure
        :return: the same DRS in the fields mode
        """
        drs.set_fields_mode()
        return drs

    def table(self, drs: DRS) -> DRS:
        """
        Given a DRS, it configures it to the table view
        :param drs: the DRS to configure
        :return: the same DRS in the table mode
        """
        drs.set_table_mode()
        return drs

    """
    Primitive API
    """

    def keyword_search(self, kw: str, max_results=10) -> DRS:
        """
        Performs a keyword search over the content of the data
        :param kw: the keyword to search
        :param max_results: the maximum number of results to return
        :return: returns a DRS
        """
        hits = store_client.search_keywords(kw, KWType.KW_CONTENT, max_results)
        drs = DRS([x for x in hits], Operation(OP.KW_LOOKUP, params=[kw]))  # materialize generator
        return drs

    def keywords_search(self, kws: [str], max_results=10) -> DRS:
        """
        Given a collection of keywords, it returns the matches in the internal representation
        :param kws: collection (iterable) of keywords (strings)
        :return: the matches in the internal representation
        """
        o_drs = DRS([], Operation(OP.NONE))
        for kw in kws:
            res_drs = self.keyword_search(kw, max_results=max_results)
            o_drs = o_drs.absorb(res_drs)
        return o_drs

    def schema_name_search(self, kw: str, max_results=10) -> DRS:
        """
        Performs a keyword search over the attribute/field names of the data
        :param kw: the keyword to search
        :param max_results: the maximum number of results to return
        :return: returns a DRS
        """
        hits = store_client.search_keywords(kw, KWType.KW_SCHEMA, max_results)
        drs = DRS([x for x in hits], Operation(
            OP.SCHNAME_LOOKUP, params=[kw]))  # materialize generator
        return drs

    def schema_names_search(self, kws: [str], max_results=10) -> DRS:
        """
        Given a collection of schema names, it returns the matches in the internal representation
        :param kws: collection (iterable) of keywords (strings)
        :return: a DRS
        """
        o_drs = DRS([], Operation(OP.NONE))
        for kw in kws:
            res_drs = self.schema_name_search(kw, max_results=max_results)
            o_drs = o_drs.absorb(res_drs)
        return o_drs

    def table_name_search(self, kw: str, max_results=10) -> DRS:
        """
        Performs a keyword search over the names of the tables
        :param kw: the keyword to search
        :param max_results: the maximum number of results to return
        :return: returns a DRS
        """
        hits = store_client.search_keywords(kw, KWType.KW_TABLE, max_results)
        drs = DRS([x for x in hits], Operation(OP.KW_LOOKUP, params=[kw]))  # materialize generator
        return drs

    def table_names_search(self, kws: [str], max_results=10) -> DRS:
        """
        Given a collection of schema names, it returns the matches in the internal representation
        :param kws: collection (iterable) of keywords (strings)
        :return: a DRS
        """
        o_drs = DRS([], Operation(OP.NONE))
        for kw in kws:
            res_drs = self.table_name_search(kw, max_results=max_results)
            o_drs = o_drs.absorb(res_drs)
        return o_drs

    def entity_search(self, kw: str, max_results=10) -> DRS:
        """
        Performs a keyword search over the entities represented by the data
        :param kw: the keyword to search
        :param max_results: the maximum number of results to return
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        hits = store_client.search_keywords(
            kw, KWType.KW_ENTITIES, max_results)
        drs = DRS([x for x in hits], Operation(
            OP.ENTITY_LOOKUP, params=[kw]))  # materialize generator
        return drs

    def schema_neighbors(self, field: (str, str, str)) -> DRS:
        """
        Returns all the other attributes/fields that appear in the same relation than the provided field
        :param field: the provided field
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        db_name, source_name, field_name = field
        hits = self.__network.get_hits_from_table(source_name)
        origin_hit = Hit(id_from(db_name, source_name, field_name), db_name, source_name, field_name, 0)
        o_drs = DRS([x for x in hits], Operation(OP.TABLE, params=[origin_hit]))
        return o_drs

    def schema_neighbors_of(self, i_drs: DRS) -> DRS:
        o_drs = DRS([], Operation(OP.NONE))
        o_drs = o_drs.absorb_provenance(i_drs)
        if i_drs.mode == DRSMode.TABLE:
            i_drs.set_fields_mode()
            for h in i_drs:
                fields_table = self.drs_from_table_hit(h)
                i_drs = i_drs.absorb(fields_table)
        for h in i_drs:
            hits = self.__network.get_hits_from_table(h.source_name)
            hits_drs = DRS([x for x in hits], Operation(OP.TABLE, params=[h]))
            o_drs = o_drs.absorb(hits_drs)
        return o_drs

    def similar_schema_name_to_field(self, field: (str, str, str)) -> DRS:
        """
        Returns all the attributes/fields with schema names similar to the provided field
        :param field: the provided field
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        field_drs = self.drs_from_raw_field(field)
        hits_drs = self.similar_schema_name_to(field_drs)
        return hits_drs

    def similar_schema_name_to_table(self, table: str) -> DRS:
        """
        Returns all the attributes/fields with schema names similar to the fields of the given table
        :param table: the given table
        :return: DRS
        """
        fields = self.drs_from_table(table)
        hits_drs = self.similar_schema_name_to(fields)
        return hits_drs

    def similar_schema_name_to(self, i_drs: DRS) -> DRS:
        """
        Given a DRS it returns another DRS that contains all fields similar to the fields of the input
        :param i_drs: the input DRS
        :return: DRS
        """
        o_drs = DRS([], Operation(OP.NONE))
        o_drs = o_drs.absorb_provenance(i_drs)
        if i_drs.mode == DRSMode.TABLE:
            i_drs.set_fields_mode()
            for h in i_drs:
                fields_table = self.drs_from_table_hit(h)
                i_drs = i_drs.absorb(fields_table)
        for h in i_drs:
            hits_drs = self.__network.neighbors_id(h, Relation.SCHEMA_SIM)
            o_drs = o_drs.absorb(hits_drs)
        return o_drs

    def similar_content_to_field(self, field: (str, str, str)) -> DRS:
        """
        Returns all the attributes/fields with content similar to the provided field
        :param field: the provided field
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        field_drs = self.drs_from_raw_field(field)
        hits_drs = self.similar_content_to(field_drs)
        return hits_drs

    def similar_content_to_table(self, table: str) -> DRS:
        fields = self.drs_from_table(table)
        hits_drs = self.similar_content_to(fields)
        return hits_drs

    def similar_content_to(self, i_drs: DRS) -> DRS:
        """
        Given a DRS it returns another DRS that contains all fields similar to the fields of the input
        :param i_drs: the input DRS
        :return: DRS
        """
        o_drs = DRS([], Operation(OP.NONE))
        o_drs = o_drs.absorb_provenance(i_drs)
        if i_drs.mode == DRSMode.TABLE:
            i_drs.set_fields_mode()
            for h in i_drs:
                fields_table = self.drs_from_table_hit(h)
                i_drs = i_drs.absorb(fields_table)
        for h in i_drs:
            hits_drs = self.__network.neighbors_id(h, Relation.CONTENT_SIM)
            o_drs = o_drs.absorb(hits_drs)
        return o_drs

    def inclusion_dependency_to(self, i_drs: DRS) -> DRS:
        """
        Given a DRS it returns another DRS that contains all fields similar to the fields of the input
        :param i_drs: the input DRS
        :return: DRS
        """
        o_drs = DRS([], Operation(OP.NONE))
        o_drs = o_drs.absorb_provenance(i_drs)
        if i_drs.mode == DRSMode.TABLE:
            i_drs.set_fields_mode()
            for h in i_drs:
                fields_table = self.drs_from_table_hit(h)
                i_drs = i_drs.absorb(fields_table)
        for h in i_drs:
            hits_drs = self.__network.neighbors_id(h, Relation.INCLUSION_DEPENDENCY)
            o_drs = o_drs.absorb(hits_drs)
        return o_drs

    def pkfk_field(self, field: (str, str, str)) -> DRS:
        """
        Returns all the attributes/fields that are primary-key or foreign-key candidates with respect to the
        provided field
        :param field: the providef field
        :return: returns a list of Hit elements of the form (id, source_name, field_name, score)
        """
        field_drs = self.drs_from_raw_field(field)
        hits_drs = self.pkfk_of(field_drs)
        return hits_drs

    def pkfk_table(self, table: str) -> DRS:
        fields = self.drs_from_table(table)
        hits_drs = self.pkfk_of(fields)
        return hits_drs

    def pkfk_of(self, i_drs: DRS) -> DRS:
        """
        Given a DRS it returns another DRS that contains all fields similar to the fields of the input
        :param i_drs: the input DRS
        :return: DRS
        """
        # alternative provenance propagation
        o_drs = DRS([], Operation(OP.NONE))
        o_drs = o_drs.absorb_provenance(i_drs)
        if i_drs.mode == DRSMode.TABLE:
            i_drs.set_fields_mode()
            for h in i_drs:
                fields_table = self.drs_from_table_hit(h)
                i_drs = i_drs.absorb(fields_table)
                # o_drs.extend_provenance(fields_drs)
        for h in i_drs:
            hits_drs = self.__network.neighbors_id(h, Relation.PKFK)
            o_drs = o_drs.absorb(hits_drs)
        # o_drs.extend_provenance(i_drs)
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
        assert a.mode == b.mode, "Input parameters are not in the same mode (fields, table)"
        o_drs = a.intersection(b)
        return o_drs

    def union(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are in either a or b
        :param a: an iterable object
        :param b: another iterable object
        :return: the union of the two provided iterable objects
        """
        assert a.mode == b.mode, "Input parameters are not in the same mode (fields, table)"
        o_drs = a.union(b)
        return o_drs

    def difference(self, a: DRS, b: DRS) -> DRS:
        """
        Returns elements that are in either a or b
        :param a: an iterable object
        :param b: another iterable object
        :return: the union of the two provided iterable objects
        """
        assert a.mode == b.mode, "Input parameters are not in the same mode (fields, table)"
        o_drs = a.set_difference(b)
        return o_drs

    """
    TC Primitive API
    """

    def paths_between(self, a: DRS, b: DRS, primitives, max_hops=2) -> DRS:
        """
        Is there a transitive relationship between any element in a with any element in b?
        This functions finds the answer constrained on the primitive (singular for now) that is passed
        as a parameter.
        :param a:
        :param b:
        :param primitives:
        :return:
        """
        assert(a.mode == b.mode)
        o_drs = DRS([], Operation(OP.NONE))
        o_drs.absorb_provenance(a)
        o_drs.absorb_provenance(b)
        if a.mode == DRSMode.FIELDS:
            for h1 in a:  # h1 is a Hit
                for h2 in b:  # h2 is a Hit
                    if h1 == h2:
                        return o_drs  # same source and target field
                    res_drs = self.__network.find_path_hit(h1, h2, primitives, max_hops=max_hops)
                    o_drs = o_drs.absorb(res_drs)
        elif a.mode == DRSMode.TABLE:
            for h1 in a:  # h1 is a table: str
                for h2 in b:  # h2 is a table: str
                    if h1 == h2:
                        return o_drs  # same source ant target table
                    res_drs = self.__network.find_path_table(
                        h1, h2, primitives, self, max_hops=max_hops)
                    o_drs = o_drs.absorb(res_drs)
        return o_drs

    def paths(self, a: DRS, primitives) -> DRS:
        """
        Is there any transitive relationship between any two elements in a?
        This function finds the answer constrained on the primitive (singular for now) passed as parameter
        :param a:
        :param primitives:
        :return:
        """
        o_drs = DRS([], Operation(OP.NONE))
        o_drs = o_drs.absorb_provenance(a)
        if a.mode == DRSMode.FIELDS:
            for h1 in a:  # h1 is a Hit
                for h2 in a:  # h2 is a Hit
                    if h1 == h2:
                        continue
                    res_drs = self.__network.find_path_hit(h1, h2, primitives)
                    o_drs = o_drs.absorb(res_drs)
        elif a.mode == DRSMode.TABLE:
            for h1 in a:  # h1 is a table: str
                for h2 in a:  # h2 is a table: str
                    res_drs = self.__network.find_path_table(
                        h1, h2, primitives, self)
                    o_drs = o_drs.absorb(res_drs)
        return o_drs

    def traverse(self, a: DRS, primitives, max_hops) -> DRS:
        o_drs = DRS([], Operation(OP.NONE))
        if a.mode == DRSMode.TABLE:
            print("ERROR: input mode TABLE not supported")
            return []
        fringe = [x for x in a]
        o_drs.absorb_provenance(a)
        while max_hops > 0:
            max_hops = max_hops - 1
            for h in fringe:
                hits_drs = self.__network.neighbors_id(h, primitives)
                o_drs = self.union(o_drs, hits_drs)
            fringe = [x for x in o_drs]  # grow the initial input
        return o_drs

    """
    Convenience functions
    """

    def serialize_sources_drs(drs: DRS, basedir: str, output_json: str):
        """
        Given a DRS as input, it produces a JSON file with the contents of the
        DRS
        """
        sources_str = ",".join([str(sn) for (nid, sn, fn, s) in drs])[:-1]
        json_dict = dict()
        json_dict["CSV"] = dict()
        json_dict["CSV"]["dir"] = basedir
        json_dict["CSV"]["table"] = sources_str
        json_obj = json.dumps(json_dict)
        with open(output_json, 'w') as f:
            f.write(json_obj)

    def output_raw(self, result_set):
        """
        Given an iterable object it prints the raw elements
        :param result_set: an iterable object
        """
        for r in result_set:
            print(str(r))

    def output(self, result_set):
        """
        Given an iterable object of elements of the form (nid, source_name, field_name, score) it prints
        the source and field names for every element in the iterable
        :param result_set: an iterable object
        """
        for r in result_set:
            (nid, sn, fn, s) = r
            print("source: " + str(sn) + "\t\t\t\t\t field: " + fn)

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


class ResultFormatter:

    @staticmethod
    def format_output_for_webclient(raw_output, consider_col_sel):
        """
        Format raw output into something client understands,
        mostly, enrich the data with schema and samples
        """

        def get_repr_columns(source_name, columns, consider_col_sel):
            def set_selected(c):
                if consider_col_sel:
                    if c in columns:
                        return 'Y'
                return 'N'
            # Get all fields in source_name
            all_fields = store_client.get_all_fields_of_source(source_name)
            colsrepr = []
            for (nid, sn, fn) in all_fields:
                colrepr = {
                    'colname': fn,
                    # ['fake1', 'fake2'], p.peek((fname, c), 15),
                    'samples': store_client.peek_values((sn, fn), 15),
                    'selected': set_selected(fn)
                }
                colsrepr.append(colrepr)
            return colsrepr

        entries = []
        # Group results into a dict with file -> [column]
        group_by_file = dict()
        for (fname, cname) in raw_output:
            if fname not in group_by_file:
                group_by_file[fname] = []
            group_by_file[fname].append(cname)
        # Create entry per filename
        for fname, columns in group_by_file.items():
            entry = {'filename': fname,
                     'schema': get_repr_columns(
                         fname,
                         columns,
                         consider_col_sel)
                     }
            entries.append(entry)
        return entries

    @staticmethod
    def format_output_for_webclient_ss(raw_output, consider_col_sel):
        """
        Format raw output into something client understands.
        The output in this case is the result of a table search.
        """
        def get_repr_columns(source_name, columns, consider_col_sel):
            def set_selected(c):
                if consider_col_sel:
                    if c in columns:
                        return 'Y'
                return 'N'
            # Get all fields of source_name
            all_fields = store_client.get_all_fields_of_source(source_name)
            all_cols = [fn for (nid, sn, fn) in all_fields]
            for myc in columns:
                all_cols.append(myc)
            colsrepr = []
            for c in all_cols:
                colrepr = {
                    'colname': c,
                    'samples': store_client.peek_values((source_name, c), 15),
                    'selected': set_selected(c)
                }
                colsrepr.append(colrepr)
            return colsrepr

        entries = []

        # Create entry per filename
        # for fname, columns in group_by_file.items():
        for fname, column_scores in raw_output:
            columns = [c for (c, _) in column_scores]
            entry = {'filename': fname,
                     'schema': get_repr_columns(
                         fname,
                         columns,
                         consider_col_sel)
                     }
            entries.append(entry)
        return entries


class API(DDAPI):

    def __init__(self, *args, **kwargs):
        super(API, self).__init__(*args, **kwargs)

    def init_store(self):
        # create store handler
        global store_client
        store_client = StoreHandler()


if __name__ == '__main__':
    print("Aurum API")
