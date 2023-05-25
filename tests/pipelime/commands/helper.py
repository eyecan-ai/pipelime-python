def sort_fn(sample):
    return sample.deep_get("metadata.random")


def filter_fn(sample):
    return sample.deep_get("metadata.double") == 6


def set_meta_fn(idx, sample):
    return sample.deep_get("metadata.double") == 6
