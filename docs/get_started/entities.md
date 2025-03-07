# Items And Samples

Pipelime data model comprises three basic concepts:

- Items
- Samples
- Samples Sequences

## Items

An item is a container for a single, generic data unit. It can contain whatever type of data you need and automatically handles some things for you, namely:
- Validation
- (De)-Serialization
- File
- Value caching

Currently, pipelime supports the following types of item:
- Images: `BmpImageItem`, `PngImageItem`, `JpegImageItem`, `TiffImageItem`
- Structured metadata: `JsonMetadataItem`, `YamlMetadataItem`, `TomlMetadataItem`
- Numpy arrays: `NpyNumpyItem`, `TxtNumpyItem`
- 3D models: `OBJModel3DItem`, `PLYModel3DItem`, `OFFModel3DItem`, `STLModel3DItem`, `GLBModel3DItem`
- Generic pickle-encoded python objects: `PickleItem`
- Unstructured binary data: `BinaryItem`

Note that `TiffImageItem` may indeed manage any kind of multi-dimensional numpy array.

We plan to extend the list of all supported item types and formats in the future, but in the meantime you are free to create and register your own items.

## Samples

Usually, a dataset comprises multiple types of items for each observation. For example, consider a visual segmentation dataset with rgb images, ground-truth binary masks and classification labels. When you access an rgb image, you may also need to access its corresponding binary mask or its classification label, so it makes sense to consider that triplet as a single entity, which we call *Sample*, containing the three items.

Samples are collections of items, they behave as a python dictionary, mapping string keys to their corresponding items. Besides the plain mapping methods, they provide some utilities for, e.g.:

- Validation
- Deep access (in [pydash](https://pydash.readthedocs.io/en/latest/deeppath.html) fashion)
- Key manipulation (change / rename / duplicate...)

## Samples Sequences

A sample sequence is the entity representing a full dataset, consisting of an ordered sequence of samples. It behaves as a python list, plus some utility methods for, e.g.:

- Validation
- Manipulation
- Data pipelining

Also, we provide convenient methods for parallel execution of long-running pipelines,
as well as common data interfaces, such as pytorch datasets.
