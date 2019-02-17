import unittest
import numpy as np

from dask.dataframe.utils import make_meta
from unittest.mock import MagicMock, patch

from dask_elk.elk_entities.index import Index
from dask_elk.elk_entities.node import Node
from dask_elk.elk_entities.shards import Shard
from dask_elk.reader import PartitionReader


class TestPartitionReader(unittest.TestCase):
    def setUp(self):
        self.__index = self.__prepare_index()

    @staticmethod
    def __prepare_index():
        index_name = 'test_index'
        mapping = {'col1': np.dtype(object), 'col2': np.dtype('float64')}
        node = Node(node_id='nodeid', publish_address='1.1.1.1:9200')
        shard = Shard(shard_id=0, node=node, state='started', no_of_docs=10)
        index = Index(name=index_name, mapping=mapping)
        index.add_shard(shard)

        return index

    @patch('dask_elk.reader.scan')
    def test_partition_reader_read(self, mock_scan):
        mock_elk_class = MagicMock()
        meta = make_meta(self.__index.mapping)
        reader = PartitionReader(index=self.__index,
                                 shard=self.__index.shards[0], meta=meta,
                                 elastic_class=mock_elk_class)

        delayed_obj = reader.read()
        delayed_obj.compute(scheduler='single-threaded')
        mock_elk_class.assert_called_with(hosts=['1.1.1.1:9200', ])
        mock_scan.assert_called_with(mock_elk_class(),
                                     **dict(mock_elk_class(),
                                            index=self.__index.name,
                                            doc_type='_doc',
                                            preference='_shards:{}'.format(
                                                0),
                                            query=dict({}),
                                            size=1000
                                            ))
        # Test with slice
        reader = PartitionReader(index=self.__index,
                                 shard=self.__index.shards[0], meta=meta,
                                 elastic_class=mock_elk_class, slice_id=0,
                                 slice_max=2)

        delayed_obj = reader.read()
        delayed_obj.compute(scheduler='single-threaded')
        mock_elk_class.assert_called_with(hosts=['1.1.1.1:9200', ])
        mock_scan.assert_called_with(mock_elk_class(),
                                     **dict(mock_elk_class(),
                                            index=self.__index.name,
                                            doc_type='_doc',
                                            preference='_shards:{}'.format(
                                                0),
                                            query=dict(
                                                slice=dict(id=0, max=2)
                                            ),
                                            size=1000,
                                            ))
