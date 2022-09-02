# Entities

All of Pipelime functionalities are built on top of three basic concepts:

- Items 
- Samples
- Samples Sequences

## Items

An item is basically a container for a single, generic data unit. It can contain whatever type of data you need and automatically handles some things for you, namely: 

- Validation
- Serialization
- File or Remote I/O
- Caching

Currently, pipelime supports the following types of item:

- Images, supporting some of the most commonly used formats: `BmpImageItem`, `PngImageItem`, `JpegImageItem`, `TiffImageItem`
- Structured metadata: `JsonMetadataItem`, `YamlMetadataItem`, `TomlMetadataItem`
- Numpy tensors: `NpyNumpyItem`, `TxtNumpyItem` 
- Generic pickle encoded python objects: `PickleItem`
- Non-structured binary data: `BinaryItem` 

We plan to extend the list of all supported item types and formats in the future.

## Samples

Most commonly, a dataset is contrived of multiple types of items for each observation, for example, consider a visual segmentation dataset with rgb images, ground-truth binary masks and classification labels. When you access an rgb image, you may also need to access its corresponding binary mask or its classification label, so it makes sense to consider that triplet as a single entity, which we call "Sample", containing the three items.

Samples are a collection of items, they behave as a python dictionary, mapping string keys to their corresponding item. Beside the plain mapping methods, they provide some utilities for:

- Validation
- Deep access (in pydash fashion)
- Key manipulation (key change / renaming / duplication)

## Samples Sequences
 
A sample sequence is the entity representing a full dataset, consisting of an ordered sequence of samples. It behaves as a plain python sequence, plus some utility methods for:

- Validation
- Disk or Remote I/O
- Manipulation