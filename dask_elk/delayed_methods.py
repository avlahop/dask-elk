import numpy as np
import pandas as pd
from elasticsearch.helpers import scan, bulk

from parsers import DocumentParser


def _elasticsearch_scan(client_cls, client_kwargs, meta=None, **params):
    # This method is executed in the worker's process and here we instantiate
    # the ES client as it cannot be serialized.
    # If no data are returned back an empty dataframe is returned with the same
    # fields and dtypes as meta.
    client = client_cls(**(client_kwargs or {}))
    results = []

    for hit in scan(client, **params):
        result = hit['_source']
        result['_id'] = hit['_id']

        results.append(result)
    data_frame = meta
    if results:
        data_frame = pd.DataFrame(results)
        data_frame = DocumentParser.parse_documents(data_frame, meta)
        data_frame = data_frame[meta.columns]
    return data_frame


def bulk_save(partition, client_cls, client_args, **kwargs):
    """
    Method to save a dataframe to Elasticsearch index. This method should be
    applied to all dask dataframe partitions using map_partitions method.

    :param pandas.DataFrame partition: The dataframe partition to save in ELK
    :param type client_cls: Elasticsearch client class to be instantiated
    :param dict[str, T] client_args: Arguments to be passed to Elasticsearch
    client class during instantiation.
    :param dict[str, T] kwargs: Arguments to be used for creating the bulk
    payload/objects
    :return: The partition dataframe
    :rtype: pd.DataFrame
    """
    elk_client = client_cls(**(client_args or {}))
    records = partition.to_dict(orient='records')
    actions = []

    for record in records:
        index = kwargs.get('index').format(**record)
        doc_type = kwargs.get('doc_type').format(**record)
        op_type = kwargs['action']
        for key, value in record.iteritems():
            if isinstance(value, np.ndarray):
                record[key] = record[key].tolist()
                continue

        action = dict(_index=index, _type=doc_type, _op_type=op_type)
        if op_type == 'update':
            action['_id'] = record['_id']
            del record['_id']
            action.update({'doc': record})
        else:
            action.update(record)
        actions.append(action)

    bulk(elk_client, actions, stats_only=False)

    return partition
