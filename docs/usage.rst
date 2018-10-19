Dask-ELK usage
==============
In order to user ``dask-elk``, first you need to create an instance of the DaskElasticClient

To connect to an Elasticsearch cluster in localhost:

.. code-block:: python

    from dask_elk.client import DaskElasticClient
    client = DaskElasticClient(host='localhost')


Read dataframe
--------------
To read from data from the elasticsearch:

.. code-block:: python

    my_index ='myindex'
    df = client.read(index=my_index, doc_type='_doc')

Client will do the following work:

- Identify the publish address of the cluster's nodes
- Obtain information on the index:
    - Mapping of the index
    - In how many shards is index divided
    - Which nodes contain the shards of the index

By default client will try to connect to each node containing the shards and issue the request on each node. Thus it will create number of tasks equal to the number of shards per requested index
You can increase the number of tasks by setting the number of documents per partition:

.. code-block:: python

    df = client.read(index=my_index, doc_type='_doc', number_of_docs_per_partition=1000)


The returned object is a dask dataframe.

You can also specify a dsl query to be pushed down to Elasticsearch to minimize the data returned back

.. code-block:: python

    df = client.read(query=query, index=my_index, doc_type='_doc')


The above code will push down the query to the Elasticsearch and minimize the returned data. Since ``DaskElasticClient`` uses the scroll api from Elasticsearch, aggregation queries are not supported.

Save dataframe
--------------
You can save your data back to an Elasticsearch index:

.. code-block:: python

    df = client.save(df, index='index_to_save', doc_type='_doc', action='index')

Save method uses bulk actions under the hood to save documents back to elasticsearch. The number of tasks depends on the number of paritions of the passed ``df``

You can use some python fromat functions to dynamically create indices during save (The operation is experimental) e.g assuming that timestamp is a datetime column then the folloing code will try to index documents on idices splited per day

.. code-block:: python

    index = 'index-{timestamp:%Y.%m.%d}'
    df = client.save(df, index=index, doc_type='_doc', action='index')


To update documents provided ``df`` needs to contain ``_id`` column. You should also provide ``update`` as action parameter

