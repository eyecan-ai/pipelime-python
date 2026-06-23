# Introduction

I need a refactoring of this library to implement Pydantic V3 in place of Pydantic V1. The refactoring should ensure that all existing functionality is preserved while taking advantage of the new features and improvements offered by Pydantic V3.

# Todo

* First of all you need to make Tests more fast, because they are too slow. You need to implement a FAST flag which ensure high coverage but low execution time. This flag should be used to skip slow tests when running the test suite in a fast mode.
* Scout for any deprecated features or methods in Pydantic V1 that are no longer supported in V3 and replace them with their V3 counterparts. Remove all V1 pydantic imports and replace them with V3 imports.