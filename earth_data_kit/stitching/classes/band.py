class Band(dict):
    def __init__(self, source_idx, description, dtype, nodataval, metadata=None):
        self.source_idx = source_idx
        self.description = description
        self.dtype = dtype
        self.nodataval = nodataval
        self.metadata = metadata

        dict.__init__(
            self,
            source_idx=source_idx,
            description=description,
            dtype=dtype,
            nodataval=nodataval,
            metadata=metadata,
        )
