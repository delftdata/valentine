import re
from collections import defaultdict
from datetime import datetime
from enum import Enum

from elasticsearch import Elasticsearch

import algorithms.sem_prop.config as c
from algorithms.sem_prop.api.annotation import MDHit, MDComment
from algorithms.sem_prop.api.apiutils import Hit


class KWType(Enum):
    KW_CONTENT = 0
    KW_SCHEMA = 1
    KW_ENTITIES = 2
    KW_TABLE = 3
    KW_METADATA = 4


class StoreHandler:

    # Store client
    client = None

    def __init__(self):
        """
            Uses the configuration file to create a connection to the store
            :return:
            """
        global client
        client = Elasticsearch([{'host': c.db_host, 'port': c.db_port}])

    def close(self):
        print("TODO")

    def get_path_of(self, nid):
        """
        Retrieves path to access the data source that contains nid
        :param nid: the id of the data source to locate
        :return: string with the path (filesystem path or db connector, etc)
        """
        body = {"query": {"match": {"id": str(nid)}}}
        res = client.search(index='profile', body=body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.path'
                                         ]
                            )
        if res['hits']['total'] == 0:
            print("!!!")
            print("nid not found in store: are you using the right EKG and store?")
            print("!!!")
        hits = res['hits']['hits']
        if len(hits) > 1:
            # TODO: handle some error here, nids should be unique
            print("ERROR: nid not unique when querying for path?")
        hit = hits[0]
        path = hit['_source']['path']
        return path

    def get_all_fields(self):
        """
        Reads all fields, described as (id, source_name, field_name) from the store.
        :return: a list of all fields with the form (id, source_name, field_name)
        """
        body = {"query": {"match_all": {}}}
        res = client.search(index='profile', body=body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.dbName',
                                         'hits.hits._source.sourceName',
                                         'hits.hits._source.columnName',
                                         'hits.hits._source.totalValues',
                                         'hits.hits._source.uniqueValues',
                                         'hits.hits._source.dataType']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                id_source_and_file_name = (h['_id'], h['_source']['dbName'], h['_source']['sourceName'],
                                           h['_source']['columnName'], h['_source']['totalValues'],
                                           h['_source']['uniqueValues'], h['_source']['dataType'])
                yield id_source_and_file_name
                remaining -= 1
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.dbName',
                                             'hits.hits._source.sourceName',
                                             'hits.hits._source.columnName',
                                             'hits.hits._source.totalValues',
                                             'hits.hits._source.uniqueValues',
                                             'hits.hits._source.dataType']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def get_all_fields_with(self, attrs):
        # FIXME: this function was not updated after 2 refactoring processes.
        """
        Reads all fields, described as (id, source_name, field_name) from the store.
        :return: a list of all fields with the form (id, source_name, field_name)
        """
        template = 'hits.hits._source.'
        filter_path = ['_scroll_id',
                       'hits.hits._id',
                       'hits.total',
                       'hits.hits._source.dbName',
                       'hits.hits._source.sourceName',
                       'hits.hits._source.columnName']
        for attr in attrs:
            new_filter_path = template + attr
            filter_path.append(new_filter_path)

        body = {"query": {"match_all": {}}}
        res = client.search(index='profile', body=body, scroll="10m",
                            filter_path=filter_path
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                toret = []
                toret.append(str(h['_id']))
                toret.append(h['_source']['sourceName'])
                toret.append(h['_source']['columnName'])
                for attr in attrs:
                    toret.append(h['_source'][attr])
                tuple_result = tuple(toret)
                yield tuple_result
                remaining -= 1
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=filter_path
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def exact_search_keywords(self, keywords, elasticfieldname, max_hits=15):
        """
        Like search_keywords, but returning only exact results
        :param keywords:
        :param elasticfieldname:
        :param max_hits:
        :return:
        """
        index = None
        query_body = None
        filter_path = ['hits.hits._source.id',
                       'hits.hits._score',
                       'hits.total',
                       'hits.hits._source.dbName',
                       'hits.hits._source.sourceName',
                       'hits.hits._source.columnName']
        if elasticfieldname == KWType.KW_CONTENT:
            index = "text"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"term": {"text": keywords}}}
        elif elasticfieldname == KWType.KW_SCHEMA:
            index = "profile"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"term": {"columnNameNA": keywords}}}
        elif elasticfieldname == KWType.KW_ENTITIES:
            index = "profile"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"term": {"entities": keywords}}}
        elif elasticfieldname == KWType.KW_TABLE:
            index = "profile"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"term": {"sourceNameNA": keywords}}}
        res = client.search(index=index, body=query_body,
                            filter_path=filter_path)
        if res['hits']['total'] == 0:
            return []
        for el in res['hits']['hits']:
            data = Hit(str(el['_source']['id']), el['_source']['dbName'], el['_source']['sourceName'],
                       el['_source']['columnName'], el['_score'])
            yield data

    def search_keywords(self, keywords, elasticfieldname, max_hits=15):
        """
        Performs a search query on elastic_field_name to match the provided keywords
        :param keywords: the list of keyword to match
        :param elasticfieldname: what is the field in the store where to apply the query
        :return: the list of documents that contain the keywords
        """
        index = None
        query_body = None
        filter_path = ['hits.hits._source.id',
                       'hits.hits._score',
                       'hits.total',
                       'hits.hits._source.dbName',
                       'hits.hits._source.sourceName',
                       'hits.hits._source.columnName']
        if elasticfieldname == KWType.KW_CONTENT:
            index = "text"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"match": {"text": keywords}}}
        elif elasticfieldname == KWType.KW_SCHEMA:
            index = "profile"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"match": {"columnName": keywords}}}
        elif elasticfieldname == KWType.KW_ENTITIES:
            index = "profile"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"match": {"entities": keywords}}}
        elif elasticfieldname == KWType.KW_TABLE:
            index = "profile"
            query_body = {"from": 0, "size": max_hits,
                          "query": {"match": {"sourceName": keywords}}}
        res = client.search(index=index, body=query_body,
                            filter_path=filter_path)
        if res['hits']['total'] == 0:
            return []
        for el in res['hits']['hits']:
            data = Hit(str(el['_source']['id']), el['_source']['dbName'], el['_source']['sourceName'],
                       el['_source']['columnName'], el['_score'])
            yield data

    def fuzzy_keyword_match(self, keywords, max_hits=15):
        """
        Performs a search query on elastic_field_name to match the provided keywords
        :param keywords: the list of keyword to match
        :param max_hits: maximum number of returned objects
        :return: the list of documents that contain the keywords
        """
        filter_path = ['hits.hits._source.id',
                       'hits.hits._score',
                       'hits.total',
                       'hits.hits._source.dbName',
                       'hits.hits._source.sourceName',
                       'hits.hits._source.columnName']
        index = "text"
        query_body = {
            "from": 0, "size": max_hits,
                "query": {
                    "match": {
                        "text": {
                            "query": keywords,
                            "fuzziness": "AUTO"
                        }
                    }
                }
            }
        res = client.search(index=index, body=query_body,
                            filter_path=filter_path)
        if res['hits']['total'] == 0:
            return []
        for el in res['hits']['hits']:
            data = Hit(str(el['_source']['id']), el['_source']['dbName'], el['_source']['sourceName'],
                       el['_source']['columnName'], el['_score'])
            yield data


    def suggest_schema(self, suggestion_string, max_hits=5):
        # filter_path = ['suggest.schema-suggest',
        #                'hits.hits._score',
        #                'hits.total',
        #                'hits.hits._source.dbName',
        #                'hits.hits._source.sourceName',
        #                'hits.hits._source.columnName']
        filter_path = []
        index = "text"
        query_body = {
            "suggest": {
                "schema-completion": {
                    "prefix": suggestion_string,
                    "completion": {
                        "field": "columnNameSuggest",
                        "fuzzy": {
                            "fuzziness": 3
                        },
                        "size": max_hits,
                        "skip_duplicates": True
                    }
                }
                # ,
                # "schema-suggest": {
                #     "text": suggestion_string,
                #     "term": {
                #         "field": "columnName",
                #         "sort": "score",
                #         "size": max_hits
                #     }
                # }
            }
        }
        res = client.search(index=index, body=query_body,
                            filter_path=filter_path)

        # return res

        res_completion = [((el['_source']['columnName'], el['_source']['sourceName']), el['_score']) for el in res['suggest']['schema-completion'][0]['options']]
        res = sorted(res_completion, key=lambda x: x[1])
        # res_term = [((el['_source']['columnName'], el['_source']['sourceName']), el['_score']) for el in res['suggest']['schema-suggest'][1]['options']]
        return [r for r, score in res]


    def get_all_fields_entities(self):
        """
        Retrieves all fields and entities from the store
        :return: (fields, entities)
        """
        results = self.get_all_fields_with(['entities'])
        fields = []
        ents = []
        for r in results:
            (nid, sn, fn, entities) = r
            fields.append((nid, sn, fn))
            ents.append(entities)
        return fields, ents

    def get_all_docs_from_text_with_idx_id(self, doc_id):
        """
        Reads all fields, described as (id, source_name, field_name) from the store.
        :return: a list of all fields with the form (id, source_name, field_name)
        """
        body = {"query": {"bool": {"must": [{"match": {"id": doc_id}}]}}}
        res = client.search(index='text', body=body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total'
                                         ]
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                raw_id_doc = h['_id']
                yield raw_id_doc
                remaining -= 1
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.id']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)

    def get_all_fields_text_signatures(self, network):

        def partition_ids(ids, partition_size=50):
            for i in range(0, len(ids), partition_size):
                yield ids[i:i + partition_size]

        def filter_term_vector_by_frequency(term_dict):
            # FIXME: add filter by term length
            filtered = []
            for k, v in term_dict.items():
                if len(k) > 3:
                    if v > 3:
                        try:
                            float(k)
                            continue
                        except ValueError:
                            matches = re.findall('[0-9]', k)
                            if len(matches) == 0:
                                filtered.append(k)
            return filtered

        text_signatures = []
        total = 0
        for nid in network.iterate_ids_text():
            total += 1
            if total % 100 == 0:
                print("text_sig: " + str(total))
            # We retrieve all documents indexed with the same id in 'text'
            docs = self.get_all_docs_from_text_with_idx_id(nid)
            ids = [x for x in docs]
            # partition ids so that they fit in one http request
            ids_partitions = partition_ids(ids)
            all_terms = defaultdict(int)
            for partition in ids_partitions:
                # We get the term vectors for each group of those documents
                # , body=term_body)
                ans = client.mtermvectors(
                    index='text', ids=partition, doc_type='column')
                # We merge them somehow
                found_docs = ans['docs']
                for doc in found_docs:
                    term_vectors = doc['term_vectors']
                    if 'text' in term_vectors:
                        # terms = list(term_vectors['text']['terms'].keys())
                        terms_and_freq = term_vectors['text']['terms']
                        for term, freq_dict in terms_and_freq.items():
                            # we don't care about the value
                            all_terms[term] = all_terms[term] + freq_dict['term_freq']
            filtered_term_vector = filter_term_vector_by_frequency(all_terms)
            if len(filtered_term_vector) > 0:
                data = (nid, filtered_term_vector)
                text_signatures.append(data)
        return text_signatures

    def get_all_mh_text_signatures(self):
        """
        Retrieves id-mh fields
        :return: (fields, numsignatures)
        """
        query_body = {
            "query": {"bool": {"filter": [{"term": {"dataType": "T"}}]}}}
        res = client.search(index='profile', body=query_body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.minhash']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']

        id_sig = []
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                data = (h['_id'], h['_source']['minhash'])
                id_sig.append(data)
                remaining -= 1
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.minhash']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)
        return id_sig

    def get_all_fields_num_signatures(self):
        """
        Retrieves numerical fields and signatures from the store
        :return: (fields, numsignatures)
        """
        query_body = {
            "query": {"bool": {"filter": [{"term": {"dataType": "N"}}]}}}
        res = client.search(index='profile', body=query_body, scroll="10m",
                            filter_path=['_scroll_id',
                                         'hits.hits._id',
                                         'hits.total',
                                         'hits.hits._source.median',
                                         'hits.hits._source.iqr',
                                         'hits.hits._source.minValue',
                                         'hits.hits._source.maxValue']
                            )
        scroll_id = res['_scroll_id']
        remaining = res['hits']['total']

        id_sig = []
        while remaining > 0:
            hits = res['hits']['hits']
            for h in hits:
                data = (h['_id'], (h['_source']['median'], h['_source']['iqr'],
                                   h['_source']['minValue'], h['_source']['maxValue']))
                id_sig.append(data)
                remaining -= 1
            res = client.scroll(scroll="5m", scroll_id=scroll_id,
                                filter_path=['_scroll_id',
                                             'hits.hits._id',
                                             'hits.hits._source.median',
                                             'hits.hits._source.iqr',
                                             'hits.hits._source.minValue',
                                             'hits.hits._source.maxValue']
                                )
            scroll_id = res['_scroll_id']  # update the scroll_id
        client.clear_scroll(scroll_id=scroll_id)
        return id_sig

    """
    Metadata
    """
    def add_annotation(self, author: str, text: str, md_class: str,
                       source: str, target={"id": None, "type": None},
                       tags=[]):
        """
        Adds annotation document to the elasticsearch graph.
        :param author: user or process who wrote the metadata
        :param text: free text annotation
        :param md_class: metadata class
        :param source: nid of column source
        :param target: (optional) {
            "id": nid of column target,
            "type": metadata relation
        }
        :param tags: (optional) keyword tags
        :return: an MDHit of the new annotation
        """
        timestamp = self._current_time()

        mapped_tags = []
        for tag in tags:
            mapped_tags.append({
                "author": author,
                "creation_date": timestamp,
                "tag": tag
            })

        body = {
            "author": author,
            "text": text,
            "class": md_class,
            "source": source,
            "target": target,
            "tags": mapped_tags,
            "creation_date": timestamp,
            "updated_date": timestamp
        }

        res = client.create(index='metadata', doc_type='annotation', body=body)
        hit = MDHit(res["_id"], author, md_class, text, source,
                    target["id"], target["type"])
        return hit

    def add_comment(self, author: str, text: str, md_id: str):
        """
        Adds a comment document to the elasticsearch graph, whose parent is
        the annotation document with the given md_id.
        :return: an MDComment of the new comment
        """
        res = client.search(index='metadata', doc_type='annotation',
                            body={"query": {"terms": {"_id": [md_id]}}})
        if res["hits"]["total"] == 0:
            raise ValueError("Given md_id does not exist.")

        timestamp = self._current_time()

        body = {
            "author": author,
            "text": text,
            "creation_date": timestamp
        }

        res = client.create(index='metadata', doc_type='comment', body=body,
                            parent=md_id)
        return MDComment(res["_id"], author, text, md_id)

    def search_keywords_md(self, keywords: list, max_hits=15):
        """
        Performs a search query on metadata to match the provided keywords
        :param keywords: the list of keywords to match
        :param max_hits: max number of results to return
        :return: the metadata that contain the keywords
        """
        index = "metadata"
        body = {"from": 0, "size": max_hits, "query": {
                "bool": {"should": [
                    {"match": {"text": keywords}},
                    {"nested": {"path": "tags", "query": {
                        "bool": {"should": [{"match": {"tags.tag": keywords}}]}
                    }}}
                ]}}}
        filter_path = ['hits.total',
                       'hits.hits._type',
                       'hits.hits._id',
                       'hits.hits._parent',
                       'hits.hits._source.author',
                       'hits.hits._source.class',
                       'hits.hits._source.source',
                       'hits.hits._source.target',
                       'hits.hits._source.text']

        res = client.search(index=index, body=body, filter_path=filter_path)
        if res['hits']['total'] == 0:
            return []

        for el in res['hits']['hits']:
            if el["_type"] == "comment":
                yield MDComment(el["_id"],
                                el["_source"]["author"],
                                el["_source"]["text"],
                                el["_parent"])
            elif el["_type"] == "annotation":
                yield MDHit(el["_id"],
                            el["_source"]["author"],
                            el["_source"]["class"],
                            el["_source"]["text"],
                            el["_source"]["source"],
                            el["_source"]["target"]["id"],
                            el["_source"]["target"]["type"])

    def add_tags(self, author: str, tags: list, md_id: str):
        """
        Add tags to the annotation with the given md_id.
        :param author: identifiable name of user or process
        :param tags: list of tags
        :param md_id: metadata id
        :return: an MDHit of the updated annotation
        """
        timestamp = self._current_time()

        res = client.search(index='metadata', doc_type='annotation',
                            body={"query": {"terms": {"_id": [md_id]}}})
        if res["hits"]["total"] == 0:
            raise ValueError("Given md_id does not exist.")

        source = res["hits"]["hits"][0]["_source"]

        new_tags = []
        for tag in tags:
            new_tags.append({
                "author": author,
                "creation_date": timestamp,
                "tag": tag
            })
        new_tags.extend(source["tags"])

        body = {
            "doc": {
                "updated_date": timestamp,
                "tags": new_tags
            }
        }
        res = client.update(index='metadata', doc_type='annotation', id=md_id,
                            body=body)
        return MDHit(res["_id"], author, source["class"], source["text"],
                     source["source"], source["target"]["id"],
                     source["target"]["type"])

    def get_metadata(self, nid: str=None, relation: str=None,
                     nid_is_source: bool=True):
        """
        :param nid: node id
        :param relation: the relation to search for
        :param nid_is_source: true iff nid is the source of the relation
        :return: metadata that reference the nid with the given relation, or
        all metadata if fields are empty
        """
        match_source_id = {"term": {"source": nid}}
        match_target_id = {"nested": {"path": "target", "query": {
            "bool": {"should": [{"term": {"target.id": nid}}]}
        }}}

        if nid is None:
            body = {"query": {"match_all": {}}}
        elif relation is None:
            body = {"query": {"bool": {"should": [
                match_source_id, match_target_id
            ]}}}
        else:
            match_id = match_source_id if nid_is_source else match_target_id
            body = {"query": {"bool": {"must": [
                match_id,
                {"nested": {"path": "target", "query": {
                    "bool": {"should": [{"term": {"target.type": relation}}]}
                }}}
            ]}}}

        res = client.search(index='metadata', doc_type="annotation", body=body,
                            scroll="10m", filter_path=[
                            'hits.hits._id',
                            'hits.total',
                            'hits.hits._source.author',
                            'hits.hits._source.class',
                            'hits.hits._source.source',
                            'hits.hits._source.target',
                            'hits.hits._source.text'])

        if res["hits"]["total"] == 0:
            return

        md_hits = []
        for md in res["hits"]["hits"]:
            md_hit = MDHit(md["_id"],
                           md["_source"]["author"],
                           md["_source"]["class"],
                           md["_source"]["text"],
                           md["_source"]["source"],
                           md["_source"]["target"]["id"],
                           md["_source"]["target"]["type"])
            md_hits.append(md_hit)
            yield md_hit

        for hit in md_hits:
            for comment in self.get_comments(hit.id):
                yield comment

    def get_comments(self, md_id: str):
        """
        :param md_id: metadata id of annotation
        :return: metadata comments that reference the md_id
        """

        body = {"query": {
            "has_parent": {
                "parent_type": "annotation",
                "query": {"bool": {"should": [
                    {"term": {"_id": md_id}}
                ]}}
            }
        }}

        res = client.search(index='metadata', doc_type="comment", body=body,
                            scroll="10m", filter_path=['hits.hits._id',
                            'hits.total',
                            'hits.hits._parent',
                            'hits.hits._source.author',
                            'hits.hits._source.text'])

        if res["hits"]["total"] == 0:
            return

        for md in res["hits"]["hits"]:
            yield MDComment(md["_id"],
                            md["_source"]["author"],
                            md["_source"]["text"],
                            md["_parent"])

    def delete_metadata_index(self):
        """
        Deletes the index 'metadata' and all its documents.
        """
        return client.indices.delete(index='metadata')

    def create_metadata_index(self):
        """
        Creates the index 'metadata' with 'annotation' and 'comment' document
        types and their corresponding mappings.
        """
        body = {
            "mappings": {
                "annotation": {
                    "properties": {
                        "author": {"type": "string", "index": "not_analyzed"},
                        "text": {"type": "string"},
                        "class": {"type": "string", "index": "not_analyzed"},
                        "source": {"type": "string", "index": "not_analyzed"},
                        "target": {
                            "type": "nested",
                            "properties": {
                                "id": {
                                    "type": "string",
                                    "index": "not_analyzed"
                                },
                                "type": {
                                    "type": "string",
                                    "index": "not_analyzed"
                                }
                            }
                        },
                        "tags": {
                            "type": "nested",
                            "properties": {
                                "author": {
                                    "type": "string",
                                    "index": "not_analyzed"
                                },
                                "creation_date": {
                                    "type": "date",
                                    "format": "basic_date_time_no_millis"
                                },
                                "tag": {"type": "string"}
                            }
                        },
                        "creation_date": {
                            "type": "date",
                            "format": "basic_date_time_no_millis"
                        },
                        "updated_date": {
                            "type": "date",
                            "format": "basic_date_time_no_millis"
                        }
                    }
                },
                "comment": {
                    "_parent": {"type": "annotation"},
                    "properties": {
                        "author": {"type": "string", "index": "not_analyzed"},
                        "creation_date": {
                            "type": "date",
                            "format": "basic_date_time_no_millis"
                        },
                        "text": {"type": "string"}
                    }
                }
            }
        }
        return client.indices.create(index='metadata', body=body)

    def _current_time(self):
        """
        Returns the current time in basic_date_time_no_millis format.
        """
        return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

if __name__ == "__main__":
    print("Elastic Store")
