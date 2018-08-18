import ssl
import re
from collections import defaultdict
from elasticsearch import Elasticsearch
from elasticsearch.connection import create_ssl_context
from elasticsearch.helpers import scan, bulk
from dask.dataframe.utils import make_meta
import dask.dataframe as dd
from dask import delayed
import numpy as np


from delayed_methods import _elasticsearch_scan


class IndexNotFoundException(Exception):
    pass


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
                       'scheme': self.scheme,}
        if http_auth:
            client_args.update({'http_auth':http_auth})
        elk_client = client_cls(**client_args)

        if not query:
            query = {}
        if not index:
            index = self.__index
        if not doc_type:
            doc_type = self.__doc_type

        # indexes = map(str.strip, index.split(','))
        # indexes = self.__get_existing_indices(elk_client, indexes)
        # if not indexes:
        #     raise IndexNotFoundException('No indexes found')
        indices, index_mapping = self.__get_mappings(
            index_client=elk_client.indices,
            index=index,
            doc_type=doc_type)
        index = ','.join(indices)

        meta = make_meta(index_mapping)
        for field in map(str.strip, fields_as_list.split(',')):
            if field in meta.columns:
                meta[field] = meta[field].astype(object)
        shard_info = self.__get_shard_info(elk_client, index)
        delayed_objs = []
        for index, shards in shard_info.iteritems():
            for shard_id, shard in shards.iteritems():
                no_of_docs = shard['docs']
                node_ip = shard['node']['ip']
                if self.wan_only:
                    node_ip = self.host
                node_port = shard['node']['port']
                if self.wan_only:
                    node_port = self.port
                number_of_partitions = self.__get_number_of_partitions(
                    no_of_docs)
                for id in range(number_of_partitions):
                    http_auth = None
                    if self.username and self.password:
                        http_auth = (self.username, self.password)
                    client_kwargs = {'hosts': [node_ip,],
                                     'port': int(node_port),
                                     'scheme': self.scheme,
                                     'timeout': self.timeout,
                                     'max_retries': self.max_retries,
                                     'retry_on_timeout': self.retry_on_timeout}
                    if http_auth:
                        client_kwargs.update({'http_auth': http_auth})

                    # ssl_context = create_ssl_context()
                    # ssl_context.check_hostname = False
                    # ssl_context.verify_mode = ssl.CERT_NONE

                    # client_kwargs.update({'verify_certs': self.verify_certs,
                    #                       'ssl_context': ssl_context})
                    scan_args = dict(index=index,
                                     doc_type=doc_type,
                                     preference='_shards:{}'.format(shard_id),
                                     query=dict(query), size=self.__scroll_size
                                     )

                    if number_of_partitions > 1:
                        scroll_slice = {'id': id, 'max': number_of_partitions}
                        scan_args.get('query', {}).update(
                            {'slice': scroll_slice})
                    delayed_objs.append(
                        delayed(_elasticsearch_scan)(client_cls, client_kwargs,
                                                     meta=meta,
                                                     **scan_args))
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

        data = data.map_partitions(bulk_save,  client_cls, client_args,
                                            meta=data, **bulk_arguments)

        return data

    def __get_shard_info(self, client, index):
        """
        :param index:
        :rtype: dict[str, dict[str, T]
        :return: The shard info for for the index
        """
        shards = defaultdict(dict)
        shard_info = client.search_shards(index=index)
        for shard in shard_info['shards']:
            for shard_item in shard:
                index = shard_item['index']
                shard_id = shard_item['shard']
                node_id = shard_item['node']
                state = shard_item['state']
                if state == 'STARTED':
                    shard_dict = {shard_id: {'node': {'id': node_id}}}
                    shards[index].update(shard_dict)

        # Get info on the nodes that contain the associated shards
        nodes = client.nodes
        cat = client.cat
        for index, values in shards.iteritems():
            shard_dict = values
            node_ids = []
            for shard_id, shard in shard_dict.iteritems():
                node_ids.append(shard['node']['id'])

            node_info = nodes.info(node_id=','.join(node_ids))
            for node, value in node_info['nodes'].iteritems():
                node_id = node
                ip, port = value['http']['publish_address'].split(':')

                for shard_id, shard in shard_dict.iteritems():
                    if node_id == shard['node']['id']:
                        shard['node'].update({'ip': ip, 'port': port})

            # Get info on the size of each shard
            cat_shards = cat.shards(index=index, format='json',
                                    h='shard,docs,prirep')
            for shard in cat_shards:
                if shard['prirep'] == 'p':
                    shard_id = int(shard['shard'])
                    no_of_docs = int(shard.get('docs', '0'))
                    if shard_id in shard_dict:
                        shard_dict[shard_id]['docs'] = no_of_docs

        return shards

    def __get_number_of_partitions(self, no_of_docs):
        return no_of_docs / self.__no_of_docs_per_partition + 1

    def __create_mappings(self, mappings):
        """

        :param dict[str, dict[str, T]] mappings:
        :return: dict[str, np.dtype]
        """

        index_maps = {}
        for field_name, field_type in mappings.iteritems():
            pandas_type = np.dtype(object)
            type = field_type.get('type')
            if type == 'integer' or type == 'long' or type == 'float':
                pandas_type = np.dtype('float64')
            elif type == 'date':
                pandas_type = np.dtype('datetime64[ns]')
            index_maps[field_name] = pandas_type

        index_maps['_id'] = np.dtype(object)

        return index_maps

    @staticmethod
    def __get_existing_indices(elk_client, index_list):
        """

        :param elasticsearch.Elasticsearch elk_client:
        :param list[str] index_list:
        :return: Existing indices from list
        :rtype: list[str]
        """
        indices_client = elk_client.indices
        existing_indices = []
        for index_name in index_list:
            if indices_client.exists(index=index_name):
                existing_indices.append(index_name)

        return existing_indices

    def __get_mappings(self, index_client, index, doc_type):
        try:
            resp = index_client.get_mapping(index=index, doc_type=doc_type,
                                            ignore_unavailable=True)
            indices = resp.keys()
            mappings = resp[indices[0]]['mappings'][doc_type]['properties']
            mappings = self.__create_mappings(mappings)
            return indices, mappings
        except Exception:
            raise IndexNotFoundException


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