import numpy as np
import pandas as pd
from elasticsearch.helpers import scan, bulk


def bulk_save(partition, client_cls, client_args, **kwargs):
    """
    Method to save a dataframe to Elasticsearch index. This method should be
    applied to all dask dataframe partitions using map_partitions method.

    :param pandas.DataFrame partition: The dataframe partition to save in ELK
    :param type client_cls: Elasticsearch client class to be instantiated
    :param dict[str,T] kwargs: Arguments to be used for creating the bulk
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
