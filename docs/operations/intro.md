# Introduction

In this tutorial you will learn how to better take advantage of pipelime to automate your data pipeline by means of stages, pipes and commands, which will constitute the three main building blocks of your pipeline.

Think back of the previous tutorial: the operations were all written explicitly in an imperative, unstructured fashion. While it is useful to know how to manipulate a dataset by individual low-level operators, writing your processing pipelines that way has some serious flaws:
- No parallelization - low efficiency resulting from all the code being executed on the same process, sequentially.
- No code reusability - the code has no structure, it is a simple for loop surrounded by I/O operations. Four operations that have nothing to do with each other are carried out by the same block of code, resulting in little to no code reusability. What if you want to simply invert the image item of a dataset without doing all the other stuff that was done in the previous tutorial?
- Poor machine/machine interfacing - there is a lack of any functional interface to apply that operation programmatically inside an automated pipeline.
- Poor human/machine interfacing - beside simply running the script with a python interpreter, the pipeline cannot be executed easily from a command line interface by the human user who wishes to process their data. 

