[![CircleCI](https://circleci.com/gh/avlahop/dask-elk/tree/master.svg?style=svg)](https://circleci.com/gh/avlahop/dask-elk/tree/master)

# dask-elk
Use dask to fetch data from Elasticsearch in parallel by sending the request to each shard separatelly. 

# Table of Contents
1. [Introduction](#introduction)
1. [Usage](#usage)








## Introduction <a name='introduction' />
The library tries to imitate the functionality of the ES Hadoop plugin for spark. `dask-elk` performs a parallel read across all the target indices shards.
In order to achieve that it uses Elasticsearch scrolling mechanism. 


## Usage <a name="usage" />
To use the library and read from an index:

```python
from dask_elk.client import DaskElasticClient

# First create a client
client = DaskElasticClient() # localhost Elasticsearch

index = 'my-index'
df = client.read(index=index, doc_type='_doc')
```

You can even pass a query to push down to elasticsearch, so that any filtering can be done on the Elasticsearch side. Because `dask-elk` uses scroll mechanism aggregations are not supported
```python
from dask_elk.client import DaskElasticClient

# First create a client
client = DaskElasticClient() # localhost Elasticsearch
query = {
    "query" : {
        "term" : { "user" : "kimchy" }
    }
}
index = 'my-index'
df = client.read(query=query, index=index, doc_type='_doc')
```