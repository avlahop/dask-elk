from elasticsearch import Elasticsearch
import dask.dataframe as dd

from dask_elk.elk_entities.index import IndexRegistry
from dask_elk.elk_entities.node import Node, NodeRegistry
from dask_elk.helpers import make_sequence
from dask_elk.reader import PartitionReader
from delayed_methods import bulk_save


class DaskElasticClient(object):
    """
    Basic object. Client to connect with Elasticsearch

    :param list[str]|str host: The host to connect to
    :param int port: The port to connect to
    :param elasticsearch.Elasticsearch client_klass: The class to use to
            create the Elasticsearch client instance
    :param str|None username: The username if X-pack is activated
    :param str|None password: The password if X-pack is activated
    :param bool wan_only: If client should perform Node lookup. Set to True
            if ELK behind firewall or if nodes not accessed directly
    :param dict[str,T] client_kwargs: Arguments to pass to Elasticsearch
            client object created by client_klass

    """

    def __init__(self, host='localhost', port=9200, client_klass=Elasticsearch,
                 username=None, password=None, wan_only=False,
                 **client_kwargs):
        """
        Constructor for DaskElasticClient object

        :param list[str] | str host: The host to connect to
        :param int port: The port to connect to
        :param elasticsearch.Elasticsearch client_klass: The class to use to
                create the Elasticsearch client instance
        :param str | None username: The username if X-pack is activated
        :param str | None password: The password if X-pack is activated
        :param bool wan_only: If client should perform Node lookup. Set to True
                if ELK behind firewall or if nodes not accessed directly
        :param dict[str, T] client_kwargs: Arguments to pass to Elasticsearch
                client object created by client_klass
        """
        self.__host = host
        self.__port = port
        self.__client_klass = client_klass
        self.__username = username
        self.__password = password
        self.__wan_only = wan_only
        self.__client_args = self.__create_client_args(client_kwargs)

    @property
    def client_args(self):
        return self.__client_args

    @property
    def hosts(self):
        return self.__host

    @property
    def port(self):
        return self.__port

    @property
    def username(self):
        return self.__username

    @property
    def password(self):
        return self.__password

    @property
    def wan_only(self):
        return self.__wan_only

    def read(self, query=None, index=None, doc_type=None,
             number_of_docs_per_partition=1000000, size=1000,
             fields_as_list=None,
             **kwargs):

        """
        Method to read from elasticsearch index/ices

        :param dict(str,T)|None query: The query to push down to ELK.
        :param str index: String of the index/ices to execute query on.
        :param str doc_type: Type index belongs too
        :param int number_of_docs_per_partition: Number of documents for
            each partition/task created by the readers
        :param int size: The scroll size
        :param str|None fields_as_list: Comma separated list of fields to be
            treated as object
        :param kwargs: Additional keyword arguments to pass to the search
            method of python Elasticsearch client
        :return: Dask Dataframe containing the data
        :rtype: dask.dataframe.DataFrame

        """

        fields_as_list = fields_as_list
        client_cls = self.__client_klass

        elk_client = client_cls(**self.client_args)

        if not query:
            query = {}

        # Get nodes info first
        node_registry = NodeRegistry()
        node_registry.get_nodes_from_elastic(elk_client)

        index_registry = IndexRegistry(nodes_registry=node_registry)
        index_registry.get_indices_from_elasticsearch(elk_client,
                                                      index=index,
                                                      doc_type=doc_type)

        meta = index_registry.calculate_meta()
        if fields_as_list:
            for field in map(str.strip, fields_as_list.split(',')):
                if field in meta.columns:
                    meta[field] = meta[field].astype(object)

        delayed_objs = []
        for index in index_registry.indices.itervalues():
            for shard in index.shards:
                no_of_docs = IndexRegistry.get_documents_count(elk_client,
                                                               query, index,
                                                               shard=shard)
                node = None
                if self.wan_only:
                    node = Node(node_id='master', publish_address=self.hosts)

                number_of_partitions = self.__get_number_of_partitions(
                    no_of_docs, number_of_docs_per_partition)
                for slice_id in range(number_of_partitions):
                    part_reader = self.__create_partition_reader(
                        index,
                        shard,
                        node, query,
                        doc_type,
                        meta,
                        number_of_partitions,
                        slice_id,
                        size,
                        **kwargs
                    )

                    delayed_objs.append(part_reader.read())

        return dd.from_delayed(delayed_objs, meta=meta)

    def save(self, data, index, doc_type, action='index'):
        """
        Save a dask dataframe to Elasticsearch

        :param dask.dataframe.DataFrame data: Dataframe to save into ELK
        :param str index: The index to save dataframe
        :param str doc_type: Index doc type
        :param str action: index if indexing you data 'update' if updating data
        :return: The data with the save applied on it
        :rtype: dask.dataframe.DataFrame

        """
        client_cls = self.__client_klass
        client_args = self.__client_args

        bulk_arguments = {'index': index, 'doc_type': doc_type,
                          'action': action,
                          }
        data = data.map_partitions(bulk_save, client_cls, client_args,
                                   meta=data, **bulk_arguments)

        return data

    def __create_partition_reader(self, index, shard, node, query, doc_type,
                                  meta, number_of_partitions, slice_id, size,
                                  **scan_arguments):
        """
        Create partition reader object
        :param dask_elk.elk_entities.index.Index index: Index to fetch data
        :param dask_elk.elk_entities.shard.Shard shard:
        :param dask_elk.elk_entities.node.Node node:
        :param dict[str, T] query:
        :param str doc_type:
        :param pandas.DataFrame meta:
        :param int number_of_partitions:
        :param int slice_id:
        :param int size: The scroll size
        :return: The partition reader object to read data from
        :rtype: dask_elk.reader.PartitionReader
        """

        part_reader = PartitionReader(
            index=index, shard=shard,
            meta=meta,
            node=node,
            doc_type=doc_type,
            query=query,
            scroll_size=size,
            slice_id=slice_id,
            slice_max=number_of_partitions,
            client_args=self.__client_args,
            **scan_arguments
        )

        if number_of_partitions > 1:
            part_reader = PartitionReader(
                index=index, shard=shard,
                meta=meta,
                node=node,
                doc_type=doc_type,
                query=query,
                scroll_size=size,
                slice_id=slice_id,
                slice_max=number_of_partitions,
                client_args=self.client_args,
                **scan_arguments
            )

        return part_reader

    def __get_number_of_partitions(self, no_of_docs, no_of_docs_per_partition):
        partitions = max(1, no_of_docs / no_of_docs_per_partition)
        return partitions

    def __create_client_args(self, client_kwargs):
        client_arguments = {'hosts': make_sequence(self.hosts),
                            'port': self.port}
        if self.username and self.password:
            client_arguments.update(
                {'http_auth': (self.username, self.password)})
        if client_kwargs:
            client_arguments.update(client_kwargs)
        return client_arguments
