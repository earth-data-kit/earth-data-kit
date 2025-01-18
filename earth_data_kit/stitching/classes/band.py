class Band(dict):
    def __init__(self, idx, description, dtype, metadata=None):
        self.idx = idx
        self.description = description
        self.dtype = dtype
        self.metadata = metadata

        dict.__init__(
            self, idx=idx, description=description, dtype=dtype, metadata=metadata
        )
