
| [<mark>Introduction</mark>](./toc.md) | [<mark>Convert Data To Underfolder</mark>](./convert_to_underfolder.md) | [<mark>Dataset Splitting</mark>](./dataset_splitting.md) | [<mark>**Creating New Items**</mark>](./item_creation.md) |
| :------: | :------: | :------: | :------: |

# Creating New Items

In the previous tutorials we have seen how [to create a new dataset from scratch](./convert_to_underfolder.md) and how [to split it in three subsets](./dataset_splitting.md) to train and test a MLP network.
Each sample came from the iris dataset, providing four features, ie, the length and width of the petals and the sepals. We want to train a network to classify the iris flowers according to their species using only the _area_ of the petals and sepals. Therefore, in this episode we will build a simple pipeline with a custom stage to add such new features to each sample of the dataset.
