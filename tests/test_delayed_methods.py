import unittest
from datetime import datetime

from mock import patch, MagicMock

import pandas as pd

from dask_elk.delayed_methods import bulk_save


class TestDelayedMethods(unittest.TestCase):

    @patch('dask_elk.delayed_methods.bulk')
    def test_bulk_save(self, mock_bulk):
        mock_elk_class = MagicMock()
        dataframe_partition = pd.DataFrame({'a': range(0, 10)})
        bulk_save(dataframe_partition,
                  mock_elk_class,
                  {'hosts': ['a host']},
                  index='index', doc_type='_doc', action='index')

        mock_elk_class.assert_called_with(**{'hosts': ['a host']})

        records = dataframe_partition.to_dict(orient='records')
        expected_actions = []
        for record in records:
            action = {
                '_index': 'index',
                '_type': '_doc',
                '_op_type': 'index'
            }

            action.update(record)
            expected_actions.append(action)

        mock_bulk.assert_called_once()
        mock_bulk.assert_called_with(mock_elk_class(), expected_actions,
                                     stats_only=False)

    @patch('dask_elk.delayed_methods.bulk')
    def test_bulk_save_with_formatter(self, mock_bulk):
        mock_elk_class = MagicMock()
        dataframe_partition = pd.DataFrame({'a': range(0, 10)})
        index = "'index-{a}"
        bulk_save(dataframe_partition,
                  mock_elk_class,
                  {'hosts': ['a host']},
                  index=index, doc_type='_doc', action='index')

        records = dataframe_partition.to_dict(orient='records')
        expected_actions = []
        for record in records:
            action = {
                '_index': index.format(**record),
                '_type': '_doc',
                '_op_type': 'index'
            }

            action.update(record)
            expected_actions.append(action)

        mock_bulk.assert_called_once()
        mock_bulk.assert_called_with(mock_elk_class(), expected_actions,
                                     stats_only=False)

    @patch('dask_elk.delayed_methods.bulk')
    def test_bulk_save_with_date_formatter(self, mock_bulk):
        mock_elk_class = MagicMock()
        dataframe_partition = pd.DataFrame({'a': range(0, 10)})
        dataframe_partition['timestamp'] = datetime(2012, 12, 12)
        index = 'index-{timestamp:%Y.%m.%d}'
        bulk_save(dataframe_partition,
                  mock_elk_class,
                  {'hosts': ['a host']},
                  index=index, doc_type='_doc', action='index')

        records = dataframe_partition.to_dict(orient='records')
        expected_actions = []
        for record in records:
            action = {
                '_index': 'index-2012.12.12',
                '_type': '_doc',
                '_op_type': 'index'
            }

            action.update(record)
            expected_actions.append(action)

        mock_bulk.assert_called_once()
        mock_bulk.assert_called_with(mock_elk_class(), expected_actions,
                                     stats_only=False)

    @patch('dask_elk.delayed_methods.bulk')
    def test_bulk_save_with_id_and_update(self, mock_bulk):
        mock_elk_class = MagicMock()
        dataframe_partition = pd.DataFrame({'a': range(0, 10)})
        dataframe_partition['_id'] = range(10, 20)
        index = "'index-{a}"

        bulk_save(dataframe_partition,
                  mock_elk_class,
                  {'hosts': ['a host']},
                  index=index, doc_type='_doc', action='update')

        records = dataframe_partition.to_dict(orient='records')
        expected_actions = []
        for record in records:
            action = {
                '_index': index.format(**record),
                '_type': '_doc',
                '_op_type': 'update',
                '_id': record['_id']
            }

            del record['_id']
            action.update({'doc': record})
            expected_actions.append(action)

        mock_bulk.assert_called_once()
        mock_bulk.assert_called_with(mock_elk_class(), expected_actions,
                                     stats_only=False)
