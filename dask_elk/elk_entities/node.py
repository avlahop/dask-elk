class Node(object):

    def __init__(self, node_id, publish_address,
                 node_type=('master', 'data', 'ingest')):
        """
        Representation of an elasticsearch node in the cluster
        :param str node_id: The id of the node
        :param str publish_address: The exposed address of the node
        :param tuple(str) node_type: The type of the node: master data, ingest
        """
        self.__node_id = node_id
        self.__publish_address = publish_address
        self.__node_type = node_type

    @property
    def node_id(self):
        return self.__node_id

    @property
    def publish_address(self):
        return self.__publish_address

    @property
    def node_type(self):
        return self.__node_type


class NodeRegistry(object):
    def __init__(self):
        self.__nodes = {}

    @property
    def nodes(self):
        return self.__nodes

    def get_nodes_from_elastic(self, elk_client):
        """

        :param elasticsearch.Elasticsearch elk_client: Instance of an
        Elasticsearch client to connect to
        """
        node_client = elk_client.nodes
        resp = node_client.info()
        for node_id, node_info in resp['nodes'].iteritems():
            publish_address = node_info['http']['publish_address']
            roles = node_info['roles']
            self.__nodes[node_id] = Node(node_id=node_id,
                                         publish_address=publish_address,
                                         node_type=tuple(roles))

    def get_node_by_id(self, node_id):
        """
        Get node object based on the nodes id
        :param str node_id: The id of the node
        :return: The node object
        :rtype: Node
        """

        return self.__nodes[node_id]
