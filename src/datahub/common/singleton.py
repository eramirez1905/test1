class Singleton(type):
    """
    The Singleton class implemented using metaclass concept.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        else:
            cls._instances[cls].__init__(*args, **kwargs)
        return cls._instances[cls]
