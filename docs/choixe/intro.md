# Introduction

**Choixe** is a mini-language built on top of python that adds some cool features to markup configuration files, with the intent of increasing code reusability and automatizing some aspects of writing configuration files.

- **Variables**: placeholders for something that will be defined later.
- **Imports**: append the content of another configuration file anywhere.
- **Sweeps**: exhaustive search over a parameter space.
- **Python Instances**: calling of dynamically imported python functions.
- **Loops**: foreach-like control that iterates over a collection of elements.
- **Utilities**: like generating a random uuid or getting the current datetime.

Currently supported formats include:
- YAML
- JSON

Note that any markup format that can be deserialized into python built-ins can work with **Choixe** syntax. In fact, you can use these features even with no configuration file at all, by just putting some **directives** into a plain python dictionary or list, **Choixe** does not care.

All that **Choixe** does is the following:

![](../images/choixe_exec.svg)

1. Optionally **load** a **structure** from a markup file. The structure usually consists of
   nested python **dictionaries** or **lists**, containing built-in types like integers,
   floats, booleans, strings.
2. **Parse** the structure into an **AST**, looking for a special syntactic pattern -
   called **"directive"** - in every string that is found.
3. **Process** the **AST** by visiting it recursively, resulting in a another python
   structure.
4. Optionally **write** the new structure to a markup file.

For simplicity, in the rest of this tutorial most examples will be written in YAML, as it is less verbose than JSON or python syntax.
