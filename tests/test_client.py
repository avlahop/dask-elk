import unittest

import numpy as np
import pandas as pd

from dask.dataframe.utils import make_meta
import dask.dataframe as dd

from mock import MagicMock, patch

from dask_elk.client import DaskElasticClient
from dask_elk.elk_entities.index import Index, IndexRegistry
from dask_elk.elk_entities.node import Node, NodeRegistry
from dask_elk.elk_entities.shards import Shard
from dask_elk.reader import PartitionReader


class TestClient(unittest.TestCase):
    def setUp(self):
        self.__node = Node(node_id='node_id', publish_address='1.1.1.1:9200')

    def test_default_values(self):
        client = DaskElasticClient()
        self.assertEqual(client.hosts, 'localhost')
        self.assertEqual(client.port, 9200)
        self.assertIsNone(client.username)
        self.assertIsNone(client.password)

    @patch('dask_elk.client.IndexRegistry', spec_set=IndexRegistry)
    @patch('dask_elk.client.NodeRegistry', spec_set=NodeRegistry)
    @patch('dask_elk.client.PartitionReader', spec_set=PartitionReader)
    def test_read_index_three_shards(self, mocked_part_reader,
                                     mocked_node_repo, mocked_index_repo):
        mock_elk_class = MagicMock()
        self.__setup_nodes_repo(mocked_node_repo)
        self.__setup_index_repo(mocked_index_repo, index_name='test',
                                no_of_shards=3)

        client = DaskElasticClient(host='test-host', port=9200,
                                   client_klass=mock_elk_class)
        dataframe = client.read()
        self.assertEqual(dataframe.npartitions, 3)
        self.assertEqual(dataframe.col1.dtype, np.dtype(object))
        self.assertEqual(dataframe.col2.dtype, np.dtype('float64'))

    @patch('dask_elk.client.IndexRegistry', spec_set=IndexRegistry)
    @patch('dask_elk.client.NodeRegistry', spec_set=NodeRegistry)
    @patch('dask_elk.client.PartitionReader', spec_set=PartitionReader)
    def test_read_three_shards_six_partitions(self, mocked_part_reader,
                                              mocked_node_repo,
                                              mocked_index_repo):
        mock_elk_class = MagicMock()
        self.__setup_nodes_repo(mocked_node_repo)
        self.__setup_index_repo(mocked_index_repo, index_name='test',
                                no_of_shards=3, doc_per_shard=2000000)

        client = DaskElasticClient(host='test-host', port=9200,
                                   client_klass=mock_elk_class)

        dataframe = client.read()
        self.assertEqual(dataframe.npartitions, 6)
        self.assertEqual(dataframe.col1.dtype, np.dtype(object))
        self.assertEqual(dataframe.col2.dtype, np.dtype('float64'))

    def __setup_nodes_repo(self, mocked_node_repo):
        mocked_node_repo().get_node_by_id.return_value = self.__node

    def __setup_index_repo(self, mocked_index_repo, index_name, no_of_shards=3,
                           doc_per_shard=1000):
        index = Index(name='index_name')
        for shard in range(no_of_shards):
            shard = Shard(shard_id=shard, node=self.__node,
                          no_of_docs=doc_per_shard, state='STARTED')
            index.add_shard(shard)

        mocked_index_repo().indices = {index.name: index}
        mocked_index_repo.get_documents_count.return_value = doc_per_shard

        mocked_index_repo().calculate_meta.return_value = make_meta(
            {'col1': np.dtype(object), 'col2': np.dtype('float64')})

    def test_save(self):
        mock_elk_class = MagicMock()
        client = DaskElasticClient(host='test-host', port=9200,
                                   client_klass=mock_elk_class)

        dataframe = MagicMock(spec_set=dd.DataFrame)

        client.save(dataframe, index='my-idnex-{a}',
                    doc_type='_doc', action='index')

        dataframe.map_partitions.assert_called_once()

