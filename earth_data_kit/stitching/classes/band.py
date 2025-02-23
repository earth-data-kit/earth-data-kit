class Band(dict):
    def __init__(self, idx, description, dtype, nodataval, metadata=None):
        self.idx = idx
        self.description = description
        self.dtype = dtype
        self.nodataval = nodataval
        self.metadata = metadata

        dict.__init__(
            self,
            idx=idx,
            description=description,
            dtype=dtype,
            nodataval=nodataval,
            metadata=metadata,
        )
