# Spark read schema compatible
If you got to this repository, you were, probably, searching for something like **"how to read parquet files with different schemas using spark"**. I'm betting on this because I, myself, searched for it multiple times before.

Here's the problem: there're some parquet files somewhere and I want to read them. If, for some reason there're files with mismatched schemas, Spark doesn't know how to read them. An example is a file where a column is of type `int` and another file where this same column is of type `long`.

```
[CANNOT_MERGE_SCHEMAS] Failed merging schemas:
    Initial schema:
        "STRUCT<int_type: INT, string_type: STRING, date_type: DATE>"
    Schema that cannot be merged with the initial schema:
        "STRUCT<int_type: BIGINT, string_type: STRING, date_type: DATE>"
```

But what about the `mergeSchema` option when reading parquet files? This option only works for **schema evolution** operations, like new columns. In all my tests, it will only make sure you will keep all the data, even if some files doesn't have all columns. But sadly, this option doesn't handle schema changes between files.

# How these changes can be handled?
Some might say that the data shouldn't have these kind of changes and, sure, there're cases where it's true. But when working in a Data Platform where there's no control over the data that is coming through because the application that's generating the information can change it's schema whenever it wants, the Platform should be resilient and be able to handle these kind of changes. It even happens when the data source is a database, though surely is a more controlled environment.

There are some schema changes that can be called **compatible**. This kind of changes is when no data is lost and, even though they might impact on how the data is consumed, they should be **safe** from the data preservation perspective. An example is the one given before, a column changing from `int` to `long`, no data is lost with this change. Another example, more drastic but still compatible, is a column changing from `int` to `string`.

On the other hand, there are some **incompatible** changes that can result in data loss. A simple example is changing from `float` to `int` or `long`. In this scenario, all the decimal part of the values will be lost with the conversion.

The main premise when solving this problem is
> If the data type changed, the existing data can be safely cast to the new data type. The most recent schema is the "more correct" schema.

# How to handle data type changes if Spark can't do it?
Well, Spark can do it, it just need some help.

There're 4 situations that can happen where there's this kind of schema conflict:
1. When reading a single partition. Example: `<path>/partition=202401`
1. When reading multiple partitions with a list of partitions. Example: `<path>/partition={202401,202402,202403}`
1. When reading multiple partitions with a pattern. Example: `<path>/partition=2024*`
1. When reading multiple partitions with a list of patterns. Example: `<path>/partition={2022*,2023*,2024*}`

And the schema conflict might happen in 2 scenarios:
1. The files in a partition have the same schema but they diverge between partitions
1. A single partition have files with 2 different schemas

So, to handle this problem, it's necessary to have some fallbacks if Spark fails to read the data.

## Definitions
First of all, here's a list of the definitions that will be used:
- A partition is the inner most folder that will contain the data. There's a bunch of content explaining this kind of data organization
- A pattern is an item of the list of paths being read (not necessarily the partitions). If the data is partitioned by hour (e.g. `2024021816` for `2024-02-18 16:00`), reading the path `<path>/partition={202401*,202402*,202403*}` will load all the partitions from `2024-01` to `2023-03`, but there're 3 **patterns** being used in this path

## Reading process
Here's the algorithm to read data when there're schema conflicts between the files. The paths shown are examples to make it more clearer.
1. Try to read the whole path (`<path>/partition={202401*,202402*,202403*}`)
1. If succeeds, return the data and finish
1. If didn't succeed, read the data by **pattern**
1. For each **pattern** (`<path>/partition=202401*`, `<path>/partition=202402*` and `<path>/partition=202403*`)
    1. Try to read the whole **pattern**
    1. If succeeds, cast the data to the desired schema, return it and finish
    1. If didn't succeed, read the data by **partition**
1. List all the **partitions** in the **pattern**
1. For each **partition** (`<path>/partition=2024010100`, `<path>/partition=2024010101`, `<path>/partition=2024010102`, ..., `<path>/partition=2024013123`)
    1. Try to read the whole **partition**
    1. If succeeds, cast the data to the desired schema, return it and finish
    1. If didn't succeed, read the data by **file**
1. List all the **files** in the **partition**
1. For each **file** (`<path>/partition=2024010100/file_1.parquet`, `<path>/partition=2024010100/file_2.parquet`, ..., `<path>/partition=2024010100/file_10.parquet`)
    1. Read the **file**
    1. Cast the data to the desired schema, return it and finish
1. **Union** all the data and return

So, the strategy is to read from top level to bottom level. If reading a whole pattern fails because there's a type change in it, read by partition. If reading a partition fails, read file by file. The file will never have a schema change in the middle of it. After reading all the data, cast it to the desired schema, union it all and return.

The fallback adds an overhead to the process. It's a lot slower to read file by file when compared to reading a whole partition that have 100 files. For this reason, reading at the lower levels should only be done if really necessary, when there's a schema merge error.

If there're 10 patterns to read but only 1 of them has a schema change in it, 9 will be loaded directly and only the problematic one will be loaded by partitions. And if only a single partition has a schema change in it, only this one will be loaded file by file, all the other ones will be loaded partition by partition.

It's expected that loading data this way will be slower, but at least it's doable without human intervention.

A better method would be if it was known beforehand where the schema change happens, but as this information is not available through code, the "try to read and fallback if there's an error" method is the most efficient way I found.

# Dealing with Avro format
Avro format is really difficult to handle and there are some strategies to read these kind of files efficiently.

## Reading Avro format without providing a schema
If you try to read **without providing a schema**, Spark won't identify new columns between files, it'll use the first schema it finds.
1. File 1 has 10 columns
1. File 2 has 11 columns
1. Reading both files at the same time might result in a dataframe with the schema of the first file
1. If this happens, the values for the "new column" for the data in File 2 won't be considered

## Reading Avro format providing a schema
If a schema is provided when reading the files, it'll be used when reading all the data requested.
1. File 1 has 10 columns
1. File 2 has 11 columns
1. If the schema of File 2 is provided when reading the data, no data will be lost

But this approach comes with a consequence: it'll raise an exception every time the data being read has a column type different from the provided schema.
1. Reading 10 partitions, where the first 9 have a column with `int` type, but the last one has the same column with `long` type
1. If reading the partitions with the schema of the last partition, the first 9 will raise an error indicating an incompatible schema
1. The last partition will be read correctly

If this scenario happens, the previous algorithm will result in reading the first 9 partitions file by file, even though they could be read as a whole without any incompatibility. To bypass this behavior, the schema used to read Avro files is obtained for each pattern and/or partition, avoiding the fallback if not necessary.
1. Following the same example as before, reading 10 partitions, where the first 9 have a column with `int` type, but the last one has the same column with `long` type
1. Read each partition with the schema of the **last file of each partition**
1. The first 9 will be read with the column as `int` type
1. The last one will be read with the column as `long` type

# Important considerations
- **It's necessary to be able to get the schema merging error while reading the data**, not after. Spark has it's lazy loading feature and if the schema compatibility is not checked right away, the error might be raised only when using the data, after the read function already finished
- The code if full of small details to handle different ways Spark can throw the errors, mostly caused by Avro format. The tests are designed to try to cover each of these special cases, but there may still be a case where it's not fully covered
- If the errors when reading data with incompatible schemas were more predictable with specific exceptions or log messages, the code could be a lot less complex
- Some functions were only tested through integration tests, as most of the unit tests that could be done would also be covered by higher level functions

# Usage
All these usage scenarios are tested in the `tests/test_usage.py` file.

```python
from src.read_schema_compatible import read_path

# Reading a path with partition naming. Example "some_path/part=1/"
result = read_path(
    spark=spark,
    file_format="parquet",
    path="some_path",
    partition_name="part",
    patterns_list=["1", "2"]
)

# Reading a path with partition naming and patterns with wildcards. Example "some_path/part=1*/"
result = read_path(
    spark=spark,
    file_format="parquet",
    path="some_path",
    partition_name="part",
    patterns_list=["1*", "2*"]
)

# Reading a path without partition naming. Example "some_path/1/"
result = read_path(
    spark=spark,
    file_format="parquet",
    path="some_path",
    patterns_list=["1", "2"]
)

# Reading a path without partition naming and patterns with wildcards. Example "some_path/1*/"
result = read_path(
    spark=spark,
    file_format="parquet",
    path="some_path",
    patterns_list=["1*", "2*"]
)

# Reading a path without partitions. Example "some_path/"
result = read_path(
    spark=spark,
    file_format="parquet",
    path="some_path",
)
```

Compatible file formats are:
- Avro
- CSV
- JSON
- Parquet
