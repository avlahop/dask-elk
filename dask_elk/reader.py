import pandas as pd

from dask import delayed
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan

from dask_elk.parsers import DocumentParser


class PartitionReader(object):
    def __init__(self, index, shard, meta, doc_type='_doc',
                 elastic_class=Elasticsearch, query=None,
                 id=None, max=None, scroll_size=1000,  **kwargs):
        """
        Intantiate the PartitionReader object. PartitionReader contains the
        the logic to get data from each shard (partition)
        :param dask_elk.elk_entities.index.Index index: The index object
        :param dask_elk.elk_entities.shard.Shard shard: The shard from which to
        read data
        :param pandas.DataFrame meta: Meta info for schema of returned data.
        :param str doc_type: The doc type the index belongs to
        :param elastic_class: Elasticsearch class to create client
        :param dict[str, T] query: The query to be pushed down to the ELK
        :param int id: The scroll id for concurrent scroll query. See
        https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html#sliced-scroll
        :param max: Max number of slices
        :param int scroll_size: Number of documents to fetch for eacch scroll
        request
        :param dict[str, T] kwargs: Additional arguments to pass to client.
        """

        self.__elastic_class = elastic_class
        self.__query = query
        self.__index = index
        self.__shard = shard
        self.__meta = meta
        self.__doc_type = doc_type
        self.__id = id
        self.__max = max
        self.__scroll_size = scroll_size
        self.__client_args = kwargs

    @delayed
    def read(self):
        client = self.__elastic_class(
            hosts=[self.__shard.node.publish_address, ],
            **self.__client_args)

        scan_args = dict(index=self.__index.name,
                         doc_type=self.__doc_type,
                         preference='_shards:{}'.format(self.__shard.shard_id),
                         query=dict(self.__query), size=self.__scroll_size
                         )

        if self.__id:
            # We need sliced scroll request
            scroll_slice = {'id': self.__id, 'max': self.__max}
            scan_args.get('query', {}).update(
                {'slice': scroll_slice})

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


