import numpy as np

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

        self.__get_shards_with_nodes(elk_client, index=index,
                                     doc_type=doc_type)

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

    def __get_shards_with_nodes(self, elk_client, index=None,
                                doc_type='_doc'):

        shard_info = elk_client.search_shards(index=index, doc_type=doc_type)
        for shard in shard_info['shards']:
            for shard_item in shard:
                index = self.__indices[shard_item['index']]
                shard_id = shard_item['shard']
                node_id = shard_item['node']
                state = shard_item['state']
                primary = shard_item['primary']
                node = self.__nodes_registry.get_node_by_id(node_id)
                if state == 'STARTED' and primary:
                    shard = Shard(shard_id=shard_id, node=node, state=state)
                    index.add_shard(shard)

        # Unfortunately search_shards doesn't provide the doc_count info
        shards = elk_client.cat.shards(index=index, doc_type=doc_type,
                                       format='json',
                                       h='shard,docs,prirep')

        # for now get only primary shards
        shards = filter(lambda shard: shard['prirep'] == 'p', shards)
        for shard in shards:
            index = self.__indices[shard['index']]
            shard_obj = index.get_shard_by_id(int(shard['shard']))
            docs = int(shard['docs']) if shard['docs'] else 0
            shard_obj.no_of_docs = docs
