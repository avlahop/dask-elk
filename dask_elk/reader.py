import pandas as pd

from dask import delayed
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from dask_elk.parsers import DocumentParser


class PartitionReader(object):
    def __init__(self, index, shard, meta, node=None, doc_type='_doc',
                 elastic_class=Elasticsearch, query=None,
                 slice_id=None, slice_max=None, scroll_size=1000,
                 client_args=None, **kwargs):
        """
        Intantiate the PartitionReader object. PartitionReader contains the
        the logic to get data from each shard (partition)
        :param dask_elk.elk_entities.index.Index index: The index object
        :param dask_elk.elk_entities.shard.Shard shard: The shard from which to
        read data
        :param dask_elk.elk_entities.node.Node node: The node to fetch data
        from.
        :param pandas.DataFrame meta: Meta info for schema of returned data.
        :param str doc_type: The doc type the index belongs to
        :param elastic_class: Elasticsearch class to create client
        :param dict[str, T] query: The query to be pushed down to the ELK
        :param int id: The scroll id for concurrent scroll query. See
        https://www.elastic.co/guide/en/elasticsearch/reference/current/
        search-request-scroll.html#sliced-scroll
        :param max: Max number of slices
        :param int scroll_size: Number of documents to fetch for eacch scroll
        request
        :param dict[str, T] | None client_args: Additional arguments to pass to
        client.
        :param dict[str, T] kwargs: Additional scan arguments to be passed to
        python's Elasticsearch search method.
        """

        self.__elastic_class = elastic_class
        self.__query = query
        self.__index = index
        self.__shard = shard
        self.__node = node
        self.__meta = meta
        self.__doc_type = doc_type
        self.__id = slice_id
        self.__max = slice_max
        self.__scroll_size = scroll_size
        self.__client_args = client_args
        self.__additional_scan_args = kwargs

    @delayed
    def read(self):
        client_args = {}
        if self.__client_args:
            client_args = self.__client_args.copy()
        client_args['hosts'] = [
                self.__node.publish_address if self.__node else
                self.__shard.node.publish_address]

        client = self.__elastic_class(**client_args)

        if not self.__query:
            self.__query = {}

        scan_args = dict(index=self.__index.name,
                         doc_type=self.__doc_type,
                         preference='_shards:{}'.format(self.__shard.shard_id),
                         query=dict(self.__query), size=self.__scroll_size
                         )

        if self.__max > 1:
            # We need sliced scroll request
            scroll_slice = {'id': self.__id, 'max': self.__max}
            scan_args.get('query', {}).update(
                {'slice': scroll_slice})

        if self.__additional_scan_args:
            scan_args.update(self.__additional_scan_args)

        results = []
        for hit in scan(client, **scan_args):
            result = hit['_source']
            result['_id'] = hit['_id']

            results.append(result)
        data_frame = self.__meta
        if results:
            data_frame = pd.DataFrame(results)
            data_frame = DocumentParser.parse_documents(data_frame,
                                                        self.__meta)
            data_frame = data_frame[self.__meta.columns]

        return data_frame
