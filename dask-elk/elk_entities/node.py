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
