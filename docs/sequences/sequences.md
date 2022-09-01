# Sequences

Sample Sequences are at the core of all pipelime features. When writing custom operators like stages, pipes and commands, you will often need to access individual samples and items and manipulate them. 

In this tutorial we will se how to interact with sample sequences and samples, their basic methods and their behaviour. Keep in mind that, yes, you could implement a data pipeline by just accessing and modifying individual samples and items, but that is strongly discouraged: you would either have to write a lot of boilerplate code, or miss out on a lot of features that pipelime provides automatically if you choose to follow the general framework of stages/pipes/commands.

## Creation

You can create a sample sequence by calling 