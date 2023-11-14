# CSV Compare
This is a little tool that solves a very specific problem: Comparing the content of 2 different CSV (comma separated values) files.
To do that comparison, the 2 CSV files are compared column by column, using the first column as identifier for sorting the rows.

This way you can compare huge files with millions of rows and thousands of columns that doesn't fit in memory.

## How to use it

Default comparison: rows and columns could be in different order in each of the files
``` 
csv-compare fileA.csv fileB.csv
```

Columns must be in exactly the same order in the two files
``` 
csv-compare --strict-column-order fileA.csv fileB.csv
```

Compare columns in groups of 20 to improve speed of the comparison (using more memory in exchange)
``` 
csv-compare --number-of-columns 20 fileA.csv fileB.csv
```

It's written in glorious Rust and uses [Polars lib](https://www.pola.rs/) under the hood to make the sorting and comparison.
