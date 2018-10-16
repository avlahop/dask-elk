import numpy as np
from dask.dataframe.utils import make_meta

from dask_elk.elk_entities.shards import Shard


class IndexNotFoundException(Exception):
    pass


class Index(object):

    def __init__(self, name, shards=None, mapping=None):
        """

        :param str name: The index name
        :param shards: Shards where this index is located in
        :param dict[str, np.dtype] mapping: The dataframe meta info of the
        index
        """
        self.__name = name
        self.__shards = shards
        if not shards:
            self.__shards = []
        self.__shards_dict = {}
        self.__mapping = mapping

    @property
    def name(self):
        return self.__name

    @property
    def shards(self):
        return self.__shards

    @property
    def mapping(self):
        return self.__mapping

    def add_shard(self, shard):
        """
        Add a shard object to the index
        :param dask_elk.elk_entities.shards.Shard shard: The shard to be added
        """
        self.__shards.append(shard)
        self.__shards_dict[shard.shard_id] = shard

    def get_shard_by_id(self, shard_id):
        """

        :param int shard_id: The id of the shard
        """

        return self.__shards_dict[shard_id]


class IndexRegistry(object):

    def __init__(self, nodes_registry):
        """

        :param dask_elk.elk_entities.nodes.NodeRegistry node_registry: The
        registry containing info on the clusters nodes
        """

        self.__indices = {}
        self.__nodes_registry = nodes_registry

    @property
    def indices(self):
        return self.__indices

    @indices.setter
    def indices(self, value):
        self.__indices = value

    def calculate_meta(self):
        """
        Since Elasticsearch is schemaless it is possible that to indices might
        have different mappings for the same "type" of documents. To get by
        this, merge all meta's together. During reading the Parsers will be
        responsible for handling mssing or different type of data.
        :return: Empty dataframe containing the expected schema
        :rtype: pandas.DataFrame
        """
        meta = {}
        for index in self.indices.itervalues():
            meta.update(index.mapping)

        meta_df = make_meta(meta)
        return meta_df

    def get_indices_from_elasticsearch(self, elk_client, index=None,
                                       doc_type='_doc'):
        """
        Get a list of index objects with its respective mappings
        :param elasticsearch.Elasticsearch elk_client: The Elasticsearch client
        instance
        :param str index: String of comma seperated indices
        :param str doc_type: The doc type of the indices
        """
        self.__indices = self.__get_mappings(elk_client.indices, index=index,
                                             doc_type=doc_type)

        self.__get_shards_with_nodes(elk_client, index=index)

    def __get_mappings(self,
                       index_client,
                       index,
                       doc_type='_doc'):

        try:
            resp = index_client.get_mapping(index=index, doc_type=doc_type,
                                            ignore_unavailable=True)

            indices_dict = {}
            for index_key, mappings in resp.iteritems():
                mapping = mappings['mappings'][doc_type]['properties']
                mapping = self.__create_mappings_dtypes(mapping)

                index_obj = Index(name=index_key, mapping=mapping)
                indices_dict[index_key] = index_obj
            return indices_dict
        except Exception:
            raise IndexNotFoundException

    @staticmethod
    def __create_mappings_dtypes(mappings):
        """
        Create dictionary with dtypes for each index field. Not supporting
        nested fields yet.
        :param dict[str, dict[str, T]] mappings:
        :return: dict[str, np.dtype]
        """

        index_maps = {}
        for field_name, field_type in mappings.iteritems():
            pandas_type = np.dtype(object)
            type = field_type.get('type')

            # Treat all numbers as floats to allow None values
            if type == 'integer' or type == 'long' or type == 'float':
                pandas_type = np.dtype('float64')
            elif type == 'date':
                pandas_type = np.dtype('datetime64[ns]')
            index_maps[field_name] = pandas_type

        index_maps['_id'] = np.dtype(object)

        return index_maps

    def __get_shards_with_nodes(self, elk_client, index=None):

        shard_info = elk_client.search_shards(index=index)
        for shard in shard_info['shards']:
            for shard_item in shard:
                index_obj = self.__indices[shard_item['index']]
                shard_id = shard_item['shard']
                node_id = shard_item['node']
                state = shard_item['state']
                primary = shard_item['primary']
                node = self.__nodes_registry.get_node_by_id(node_id)
                if state == 'STARTED' and primary:
                    shard = Shard(shard_id=shard_id, node=node, state=state)
                    index_obj.add_shard(shard)

    @staticmethod
    def get_documents_count(elk_client, query, index, doc_type='_doc',
                            shard=None):
        """
        Method to get the document count for a specified query on a specified
        index. If shard is provided then the query is executed on that
        specific shard
        :param elasticsearch.Elasticsearch elk_client: Elasticsearch client to
        use
        :param dict[str, T] query: Query to push down to elasticsearch
        :param dask_elk.elk_entities.index.Index index: The index object to
        execute query on
        :param str doc_type: The doc type the index belongs to
        :param dask_elk.elk_entities.shards.Shard | None shard: The shard to
        execute the query on
        :return: The number of documents
        :rtype: int
        """
        preference = None
        if shard:
            preference = '_shards:{}'.format(shard.shard_id)

        count_argurments = {'body': query, 'index': index.name,
                            'doc_type': doc_type}

        if preference:
            count_argurments.update({'preference': preference})

        no_of_documents = elk_client.count(**count_argurments)

        return no_of_documents['count']
