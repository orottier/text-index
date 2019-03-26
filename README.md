# text_index

Blazing fast csv file indexing, persisting and querying

## What?

This utility starts making sense when you are dealing with 20GB+ csv files, that need some manual pre-processing before moving into your database, or that you just want to tinker with.

You can build an index for any of your csv columns, as text, integer, or float type. The index will be stored on disk, typically about 3% of the original size for a text column. Parsing is performed with the excellent `csv` crate, processing about 800K records/sec (all threads combined) on my 2014 macbook when indexing a 64 column file.

Querying should typically be in the order of ~100 ms for equality lookups (ranges are slower). The index is stored sharded, so lookup times should not increase dramatically with input size.

## Usage

### Build the index

You can choose to index a column as text (str), integer (int) or floating point (float).

```
USAGE:
    text_index <INPUT> index <COLUMN> [TYPE]

OPTIONS:
    -t <THREADS>     Max number of THREADS
    -v               Verbose output (-v, -vv supported)

ARGS:
    <COLUMN>    Column number (starts at 1)
    <TYPE>      Type (str(default), int, float)
```

e.g. `text_index input.csv -t 4 index 1 str`

### Query the index

```
USAGE:
    text_index <INPUT> filter <COLUMN> <OP> <VALUE> [VALUE2]

ARGS:
    <COLUMN>    Column number (starts at 1)
    <OP>        Operator (eq, lt, le, gt, ge, in, pre (starts with))
    <VALUE>     Value
    <VALUE2>    Value2 (when operator is `in`)
```

e.g. `text_index input.csv filter 1 eq "search_string"`

## The future

- Support more text file formats, such as newline delimited json, or log files
- Multithreaded querying
- Support gzipped input files
- Swap friendly indexing (limit memory usage)
