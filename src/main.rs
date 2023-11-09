use clap::Parser;
use colored::*;
use indicatif::ProgressBar;
use polars::frame::DataFrame;
use polars::prelude::{col, IndexOfSchema, IntoVec, LazyCsvReader, LazyFileListReader, LazyFrame, SortOptions};
use std::collections::HashSet;
use std::process::exit;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// First file to compare
    file1: String,

    /// Second file to compare
    file2: String,

    /// Whether files are required to have the columns in the same order (default: allow unordered)
    #[arg(default_value = "false", long, short)]
    strict_column_order: bool,
}

fn main() {
    let args = Args::parse();

    let first_file_lf = get_lazy_frame(args.file1.as_str());
    let second_file_lf = get_lazy_frame(args.file2.as_str());

    assert_both_frames_have_same_row_num(&first_file_lf, &second_file_lf);

    let first_file_cols = get_column_names(&first_file_lf);
    let second_file_cols = get_column_names(&second_file_lf);

    assert_both_frames_are_comparable(
        &first_file_cols,
        &second_file_cols,
        args.strict_column_order,
    );

    let sorting_column = &first_file_cols[0];
    let columns_to_iterate = (first_file_cols.len() - 1) as u64;

    println!(
        "Comparing content of each column in both files when sorted by column \"{}\"{}...",
        sorting_column,
        if args.strict_column_order {" . Strict order of columns enforced"} else {""}
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

fn assert_both_frames_have_same_row_num(first_lazy_frame: &LazyFrame,
                                        second_lazy_frame: &LazyFrame) {
    let first_rows_num = get_rows_num(first_lazy_frame);
    let second_rows_num = get_rows_num(second_lazy_frame);

    if first_rows_num != second_rows_num {
        println!(
            "{}: {} {} <> {}",
            "FILES ARE DIFFERENT".red(),
            "Different number of rows".red(),
            first_rows_num.to_string(),
            second_rows_num.to_string()
        );

        exit(4);
    }
}

fn assert_both_frames_are_comparable(
    first_file_cols: &[String],
    second_file_cols: &[String],
    is_strict_order: bool,
) {
    let have_same_columns = if is_strict_order {
        first_file_cols.eq(second_file_cols)
    } else {
        // Convert the vectors into sets to ignore the order
        let set1: HashSet<_> = first_file_cols.iter().collect();
        let set2: HashSet<_> = second_file_cols.iter().collect();
        set1 == set2
    };

    if !have_same_columns {
        println!(
            "{}: {} => [{}] != [{}]",
            "FILES ARE DIFFERENT".red(),
            "Different columns".red(),
            first_file_cols.join(",").bold().yellow(),
            second_file_cols.join(",").bold().blue()
        );
        if is_strict_order {
            println!(
                "{} {} {}",
                "Hint:",
                "--strict-order".bold(),
                "flag is active"
            );
        }
        exit(2);
    }
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

fn get_rows_num(lazy_frame: &LazyFrame) -> u32 {
    let first_column_name = get_column_names(&lazy_frame.clone())[0].to_string();
    return lazy_frame.clone()
        .select([col(first_column_name.as_str())])
        .collect().expect("Error when counting the rows of the CSV file")
        .shape().0 as u32;
}
