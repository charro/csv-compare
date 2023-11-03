use colored::*;
use indicatif::ProgressBar;
use polars::frame::DataFrame;
use polars::prelude::{
    col, IndexOfSchema, IntoVec, LazyCsvReader, LazyFileListReader, LazyFrame, SortOptions,
};
use std::env;
use std::process::exit;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const EXECUTABLE_NAME: &str = env!("CARGO_PKG_NAME");

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        println!("-- CSV Compare v{}. Usage:\n", VERSION);
        println!("  {} file1.csv file2.csv\n", EXECUTABLE_NAME);
        exit(1);
    }

    let first_file_path = &args[1];
    let second_file_path = &args[2];

    let first_file_lf = get_lazy_frame(first_file_path);
    let second_file_lf = get_lazy_frame(second_file_path);

    let first_file_cols = get_column_names(&first_file_lf);
    let second_file_cols = get_column_names(&second_file_lf);

    if first_file_cols.len() != second_file_cols.len() {
        println!(
            "{} : {}",
            "FILES ARE DIFFERENT".red(),
            "Different number of columns".on_bright_red()
        );
        exit(2);
    }

    for i in 0..first_file_cols.len() {
        if first_file_cols[i] != second_file_cols[i] {
            println!(
                "{}: {} #{} => {} != {}",
                "FILES ARE DIFFERENT".red(),
                "Different names for column".red(),
                i + 1,
                first_file_cols[i].bold().yellow(),
                second_file_cols[i].bold().blue()
            );
            exit(2);
        }
    }

    let sorting_column = &first_file_cols[0];
    let columns_to_iterate = (first_file_cols.len() - 1) as u64;

    println!(
        "Comparing content of each column in both files when sorted by column: {} ...",
        sorting_column
    );
    let progress_bar = ProgressBar::new(columns_to_iterate);
    for i in 1..first_file_cols.len() {
        let column_name = &first_file_cols[i];

        let first_data_frame =
            get_sorted_data_frame_for_column(&first_file_lf, sorting_column, column_name);

        let second_data_frame =
            get_sorted_data_frame_for_column(&second_file_lf, sorting_column, column_name);

        if !first_data_frame.frame_equal_missing(&second_data_frame) {
            println!(
                "{}: {} {} {}",
                "FILES ARE DIFFERENT".red(),
                "Values for column".red(),
                column_name.on_bright_red(),
                "are different".red()
            );

            exit(3);
        }
        progress_bar.inc(1);
    }
    progress_bar.finish();

    println!(
        "{} {}",
        "FILES ARE IDENTICAL WHEN SORTED BY COLUMN:".green(),
        sorting_column.green()
    );
}

fn get_lazy_frame(file_path: &str) -> LazyFrame {
    LazyCsvReader::new(file_path)
        .has_header(true)
        .with_infer_schema_length(Some(0))
        .finish()
        .expect(format!("Couldn't open file {file_path}").as_str())
}

fn get_column_names(lazy_frame: &LazyFrame) -> Vec<String> {
    let schema = lazy_frame
        .clone()
        .limit(1)
        .collect()
        .expect("Couldn't parse first CSV file")
        .schema();

    schema.get_names().into_vec()
}

fn get_sorted_data_frame_for_column(
    lazy_frame: &LazyFrame,
    sorting_column: &String,
    column: &String,
) -> DataFrame {
    lazy_frame
        .clone()
        .select([col(sorting_column), col(column)])
        .sort(sorting_column, SortOptions::default())
        .collect()
        .expect(format!("Couldn't sort column {column} by column {sorting_column}",).as_str())
}
