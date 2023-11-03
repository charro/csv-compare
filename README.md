# CSV Compare
This is a little tool that solves a very specific problem: Comparing the content of 2 different CSV (comma separated values) files, taking in account that the rows and columns could be in different order. Also there's a way to enforce the strict order of columns (**--strict-order** flag).

To do that comparison, the 2 CSV files are compared column by column, using the first column as identifier for sorting.

This way you can compare huge files with millions of rows and thousands of columns that doesn't fit in memory.

It's written in glorious Rust and uses [Polars lib](https://www.pola.rs/) under the hood.
