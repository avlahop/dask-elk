class Shard(object):
    def __init__(self, shard_id, node, state, no_of_docs=0):
        self.__shard_id = shard_id
        self.__node = node
        self.__state = state
        self.__no_of_docs = no_of_docs

    @property
    def shard_id(self):
        return self.__shard_id

    @property
    def node(self):
        return self.__node

    @property
    def state(self):
        return self.__state

    @property
    def no_of_docs(self):
        return self.__no_of_docs

    @no_of_docs.setter
    def no_of_docs(self, value):
        self.__no_of_docs = value
