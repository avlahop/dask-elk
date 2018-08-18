class Index(object):
    def __init__(self, name, shards=None):
        self._name = name
        self.__shards = shards

    @property
    def name(self):
        return self.__name

    @property
    def shards(self):
        return self.__shards