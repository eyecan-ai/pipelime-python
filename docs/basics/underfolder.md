# Underfolder

By default, pipelime reads and writes data in the *Underfolder* format, a flexible file-system based dataset storage format.

The main benefits of using the underfolder format are:

- Flexibility: one single format for pretty much all the datasets that you will ever use.
- Readability: no need to use special viewers to access the dataset, all data is stored as separate files with common extensions, making it easy to manually inspect.
- Low disk usage: pipelime takes advantage of hard-links and item sharing to minimize disk usage.

An underfolder structure is here summarized:

![underfolder structure](../images/underfolder.png "underfolder structure")

Every underfolder must contain a subfolder named `data`. Inside this subfolder, all items are stored as separate files, with a specific naming rule:

![naming convention](../images/naming.png "naming convention")

Alongside the `data` subfolder the underfolder dataset can contain *root files*, i.e. some common items that are automatically inherited by all samples, without the need to replicate them and clutter your file-system.
