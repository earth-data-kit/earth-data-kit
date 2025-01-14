class Band(dict):
    def __init__(self, idx, description, dtype, x_size, y_size, metadata=None):
        self.idx = idx
        self.description = description
        self.dtype = dtype
        self.x_size = x_size
        self.y_size = y_size
        self.metadata = metadata

        dict.__init__(self, idx=idx, description=description, dtype=dtype, x_size=x_size, y_size=y_size, metadata=metadata)
