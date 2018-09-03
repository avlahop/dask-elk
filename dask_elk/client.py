from elasticsearch import Elasticsearch
import dask.dataframe as dd

from dask_elk.elk_entities.index import IndexRegistry
from dask_elk.elk_entities.node import Node, NodeRegistry
from dask_elk.reader import PartitionReader
from delayed_methods import bulk_save


class DaskElasticClient(object):
    def __init__(self, host, port=9200, index=None, doc_type=None,
                 client_klass=Elasticsearch, username=None, password=None,
                 **kwargs):
        self.__index = index
        self.__host = host
        self.__port = port
        self.__client_klass = client_klass
        self.__doc_type = doc_type
        self.__no_of_docs_per_partition = 100000
        self.__scroll_size = 1000
        self.__username = username
        self.__password = password
        self.__wan_only = kwargs.get('wan_only', False)
        self.__scheme = kwargs.get('scheme', 'http')
        self.__verify_certs = kwargs.get('verify_certs', True)
        self.__timeout = kwargs.get('timeout', 600)
        self.__max_retries = kwargs.get('max_retries', 10)
        self.__retry_on_timeout = kwargs.get('retry_on_timeout', True)

    @property
    def index(self):
        return self.__index

    @index.setter
    def index(self, value):
        self.__index = value

    @property
    def host(self):
        return self.__host

    @property
    def port(self):
        return self.__port

    @property
    def doc_type(self):
        return self.__doc_type

    @property
    def no_doc_per_partition(self):
        return self.__no_of_docs_per_partition

    @property
    def username(self):
        return self.__username

    @property
    def password(self):
        return self.__password

    @property
    def wan_only(self):
        return self.__wan_only

    @property
    def scheme(self):
        return self.__scheme

    @property
    def verify_certs(self):
        return self.__verify_certs

    @property
    def timeout(self):
        return self.__timeout

    @property
    def max_retries(self):
        return self.__max_retries

    @property
    def retry_on_timeout(self):
        return self.__retry_on_timeout

    def read(self, query=None, index=None, doc_type=None, **kwargs):
        fields_as_list = kwargs.get('fields_as_list', '')
        client_cls = self.__client_klass
        http_auth = None
        if self.username and self.password:
            http_auth = (self.username, self.password)

        # ssl_context = create_ssl_context()
        # ssl_context.check_hostname = False
        # ssl_context.verify_mode = ssl.CERT_NONE
        client_args = {'hosts': [self.host, ], 'port': self.port,
                       'scheme': self.scheme}
        if http_auth:
            client_args.update({'http_auth': http_auth})
        elk_client = client_cls(**client_args)

        if not query:
            query = {}
        if not index:
            index = self.__index
        if not doc_type:
            doc_type = self.__doc_type

        # Get nodes info first
        node_registry = NodeRegistry()
        node_registry.get_nodes_from_elastic(elk_client)

        index_registry = IndexRegistry(nodes_registry=node_registry)
        index_registry.get_indices_from_elasticsearch(elk_client,
                                                      index=index,
                                                      doc_type=doc_type)

        meta = index_registry.calculate_meta()

        for field in map(str.strip, fields_as_list.split(',')):
            if field in meta.columns:
                meta[field] = meta[field].astype(object)

        delayed_objs = []
        for index in index_registry.indices.itervalues():
            for shard in index.shards:
                no_of_docs = shard.no_of_docs
                node = None
                if self.wan_only:
                    node = Node(node_id='master', publish_address=self.host)

                number_of_partitions = self.__get_number_of_partitions(
                    no_of_docs)
                for slice_id in range(number_of_partitions):
                    part_reader = self.__create_partition_reader(
                        index,
                        shard,
                        node, query,
                        doc_type,
                        meta,
                        number_of_partitions,
                        slice_id)

                    delayed_objs.append(part_reader.read())

        return dd.from_delayed(delayed_objs, meta=meta)

    def save(self, data, index, doc_type, action='index',
             dynamic_write_options=None):
        """

        :param dask.dataframe.DataFrame data:
        :param str index:
        :param str doc_type:
        :param str action:
        :param dict[str, T] dynamic_write_options:
        :return:
        """
        client_cls = self.__client_klass
        client_args = {'hosts': [self.host, ], 'port': self.port,
                       'scheme': self.scheme, }
        http_auth = None
        if self.username and self.password:
            http_auth = (self.username, self.password)
            client_args.update({'http_auth': http_auth})

        bulk_arguments = {'index': index, 'doc_type': doc_type,
                          'action': action,
                          'dynamic_write_options': dynamic_write_options}

        data = data.map_partitions(bulk_save, client_cls, client_args,
                                   meta=data, **bulk_arguments)

        return data

    def __create_partition_reader(self, index, shard, node, query, doc_type,
                                  meta, number_of_partitions, slice_id):
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
        :return: The partition reader object to read data from
        :rtype: dask_elk.reader.PartitionReader
        """
        http_auth = None
        if self.username and self.password:
            http_auth = (self.username, self.password)
        client_kwargs = {'scheme': self.scheme,
                         'timeout': self.timeout,
                         'max_retries': self.max_retries,
                         'retry_on_timeout': self.retry_on_timeout}
        if http_auth:
            client_kwargs.update({'http_auth': http_auth})

        part_reader = PartitionReader(
            index=index, shard=shard,
            meta=meta,
            node=node,
            doc_type=doc_type,
            query=query,
            scroll_size=self.__scroll_size,
            **client_kwargs)

        if number_of_partitions > 1:
            part_reader = PartitionReader(
                index=index, shard=shard,
                meta=meta,
                node=node,
                doc_type=doc_type,
                query=query,
                scroll_size=self.__scroll_size,
                slice_id=slice_id,
                slice_max=number_of_partitions,
                **client_kwargs)

        return part_reader

    def __get_number_of_partitions(self, no_of_docs):
        partitions = no_of_docs / self.__no_of_docs_per_partition
        modulo = no_of_docs % self.__no_of_docs_per_partition
        partitions = partitions + 1 if modulo > 0 else partitions
        return partitions


def get_indices_for_period_of_time(period_of_time, index_prefix,
                                   format_string):
    """

    :param swissknife.datetime.TimeWindow period_of_time:
    :param index_prefix:
    :param format_string:
    :return:
    """
    time_windows = period_of_time.split_per_day()
    indices = []
    for time_window in time_windows:
        index = '-'.join(
            [index_prefix, time_window.since.strftime(format_string)])
        indices.append(index)
    return ','.join(indices)
