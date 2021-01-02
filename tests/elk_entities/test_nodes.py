import unittest

from unittest.mock import MagicMock

from dask_elk.elk_entities.node import NodeRegistry


class TestNodeRegistry(unittest.TestCase):
    def setUp(self):
        self.elk_client = MagicMock()
        self.elk_client.nodes.info.return_value = self.__get_mocked_node_resp()

    def test_get_nodes_from_elastic(self):
        node_registry = NodeRegistry()
        node_registry.get_nodes_from_elastic(self.elk_client)
        self.assertEqual(1, len(node_registry.nodes))
        self.assertIn("E43TT5r9Q9-VWckuAXLf0Q", node_registry.nodes)
        node = node_registry.nodes["E43TT5r9Q9-VWckuAXLf0Q"]
        self.assertEqual("E43TT5r9Q9-VWckuAXLf0Q", node.node_id)
        self.assertEqual("1.1.1.1:9200", node.publish_address)
        self.assertEqual(("master", "data", "ingest"), node.node_type)

    def test_get_nodes_from_elastic_raises_key_error(self):
        node_registry = NodeRegistry()
        self.elk_client.nodes.info.return_value = (
            self.get_mocked_node_resp_no_pub_address()
        )
        with self.assertRaises(Exception) as exc_context:
            node_registry.get_nodes_from_elastic(self.elk_client)
        exception = exc_context.exception
        self.assertEqual(
            "Could not get publish address. Try wan_only "
            "option when creating a client",
            str(exception),
        )

    def __get_mocked_node_resp(self):
        return {
            "_nodes": {"total": 1, "successful": 1, "failed": 0},
            "cluster_name": "docker-cluster",
            "nodes": {
                "E43TT5r9Q9-VWckuAXLf0Q": {
                    "name": "E43TT5r",
                    "transport_address": "1.1.1.1:9300",
                    "host": "1.1.1.1",
                    "ip": "1.1.1.1",
                    "version": "6.2.2",
                    "build_hash": "10b1edd",
                    "total_indexing_buffer": 103887667,
                    "roles": ["master", "data", "ingest"],
                    "settings": {
                        "cluster": {"name": "docker-cluster"},
                        "node": {"name": "E43TT5r"},
                        "path": {
                            "logs": "/usr/share/elasticsearch/logs",
                            "home": "/usr/share/elasticsearch",
                        },
                        "discovery": {
                            "type": "single-node",
                            "zen": {"minimum_master_nodes": "1"},
                        },
                        "client": {"type": "node"},
                        "http": {"type": {"default": "netty4"}},
                        "transport": {"type": {"default": "netty4"}},
                        "network": {"host": "0.0.0.0"},
                    },
                    "process": {
                        "refresh_interval_in_millis": 1000,
                        "id": 1,
                        "mlockall": "false",
                    },
                    "thread_pool": {
                        "force_merge": {
                            "type": "fixed",
                            "min": 1,
                            "max": 1,
                            "queue_size": -1,
                        },
                        "fetch_shard_started": {
                            "type": "scaling",
                            "min": 1,
                            "max": 8,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "listener": {
                            "type": "fixed",
                            "min": 2,
                            "max": 2,
                            "queue_size": -1,
                        },
                        "index": {
                            "type": "fixed",
                            "min": 4,
                            "max": 4,
                            "queue_size": 200,
                        },
                        "refresh": {
                            "type": "scaling",
                            "min": 1,
                            "max": 2,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "generic": {
                            "type": "scaling",
                            "min": 4,
                            "max": 128,
                            "keep_alive": "30s",
                            "queue_size": -1,
                        },
                        "warmer": {
                            "type": "scaling",
                            "min": 1,
                            "max": 2,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "search": {
                            "type": "fixed_auto_queue_size",
                            "min": 7,
                            "max": 7,
                            "queue_size": 1000,
                        },
                        "flush": {
                            "type": "scaling",
                            "min": 1,
                            "max": 2,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "fetch_shard_store": {
                            "type": "scaling",
                            "min": 1,
                            "max": 8,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "management": {
                            "type": "scaling",
                            "min": 1,
                            "max": 5,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "get": {
                            "type": "fixed",
                            "min": 4,
                            "max": 4,
                            "queue_size": 1000,
                        },
                        "bulk": {
                            "type": "fixed",
                            "min": 4,
                            "max": 4,
                            "queue_size": 200,
                        },
                        "snapshot": {
                            "type": "scaling",
                            "min": 1,
                            "max": 2,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                    },
                    "transport": {
                        "bound_address": ["0.0.0.0:9300"],
                        "publish_address": "1.1.1.1:9300",
                        "profiles": {},
                    },
                    "http": {
                        "bound_address": ["0.0.0.0:9200"],
                        "publish_address": "1.1.1.1:9200",
                        "max_content_length_in_bytes": 104857600,
                    },
                }
            },
        }

    def get_mocked_node_resp_no_pub_address(self):
        return {
            "_nodes": {"total": 1, "successful": 1, "failed": 0},
            "cluster_name": "docker-cluster",
            "nodes": {
                "E43TT5r9Q9-VWckuAXLf0Q": {
                    "name": "E43TT5r",
                    "transport_address": "1.1.1.1:9300",
                    "host": "1.1.1.1",
                    "ip": "1.1.1.1",
                    "version": "6.2.2",
                    "build_hash": "10b1edd",
                    "total_indexing_buffer": 103887667,
                    "roles": ["master", "data", "ingest"],
                    "settings": {
                        "cluster": {"name": "docker-cluster"},
                        "node": {"name": "E43TT5r"},
                        "path": {
                            "logs": "/usr/share/elasticsearch/logs",
                            "home": "/usr/share/elasticsearch",
                        },
                        "discovery": {
                            "type": "single-node",
                            "zen": {"minimum_master_nodes": "1"},
                        },
                        "client": {"type": "node"},
                        "http": {"type": {"default": "netty4"}},
                        "transport": {"type": {"default": "netty4"}},
                        "network": {"host": "0.0.0.0"},
                    },
                    "process": {
                        "refresh_interval_in_millis": 1000,
                        "id": 1,
                        "mlockall": "false",
                    },
                    "thread_pool": {
                        "force_merge": {
                            "type": "fixed",
                            "min": 1,
                            "max": 1,
                            "queue_size": -1,
                        },
                        "fetch_shard_started": {
                            "type": "scaling",
                            "min": 1,
                            "max": 8,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "listener": {
                            "type": "fixed",
                            "min": 2,
                            "max": 2,
                            "queue_size": -1,
                        },
                        "index": {
                            "type": "fixed",
                            "min": 4,
                            "max": 4,
                            "queue_size": 200,
                        },
                        "refresh": {
                            "type": "scaling",
                            "min": 1,
                            "max": 2,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "generic": {
                            "type": "scaling",
                            "min": 4,
                            "max": 128,
                            "keep_alive": "30s",
                            "queue_size": -1,
                        },
                        "warmer": {
                            "type": "scaling",
                            "min": 1,
                            "max": 2,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "search": {
                            "type": "fixed_auto_queue_size",
                            "min": 7,
                            "max": 7,
                            "queue_size": 1000,
                        },
                        "flush": {
                            "type": "scaling",
                            "min": 1,
                            "max": 2,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "fetch_shard_store": {
                            "type": "scaling",
                            "min": 1,
                            "max": 8,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "management": {
                            "type": "scaling",
                            "min": 1,
                            "max": 5,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                        "get": {
                            "type": "fixed",
                            "min": 4,
                            "max": 4,
                            "queue_size": 1000,
                        },
                        "bulk": {
                            "type": "fixed",
                            "min": 4,
                            "max": 4,
                            "queue_size": 200,
                        },
                        "snapshot": {
                            "type": "scaling",
                            "min": 1,
                            "max": 2,
                            "keep_alive": "5m",
                            "queue_size": -1,
                        },
                    },
                    "transport": {
                        "bound_address": ["0.0.0.0:9300"],
                        "profiles": {},
                    },
                    "http": {
                        "bound_address": ["0.0.0.0:9200"],
                        "max_content_length_in_bytes": 104857600,
                    },
                }
            },
        }
