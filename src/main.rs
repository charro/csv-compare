use clap::Parser;
use colored::*;
use indicatif::{ProgressBar, ProgressStyle};
use polars::frame::DataFrame;
use polars::prelude::{
    col, IndexOfSchema, IntoVec, LazyCsvReader, LazyFileListReader, LazyFrame, SortOptions,
};
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

    /// How many columns to compare at the same time.
    /// The bigger the number the faster, but will also increase the memory consumption
    #[arg(default_value = "1", long, short)]
    number_of_columns: usize,

    /// Column separator character
    #[arg(default_value = ",", long, short = 'p')]
    separator: char,
}

fn main() {
    let args = Args::parse();

    let first_file_path = args.file1.as_str();
    let second_file_path = args.file2.as_str();

    println!(
        "Comparing file {} with file {}. {} column(s) at a time... {}",
        first_file_path,
        second_file_path,
        args.number_of_columns,
        if args.strict_column_order {
            " Strict order of columns enforced".yellow()
        } else {
            "".white()
        }
    );

    let separator = args.separator;
    let first_file_lf = get_lazy_frame(first_file_path, separator);
    let second_file_lf = get_lazy_frame(second_file_path, separator);

    let row_num = assert_both_frames_have_same_row_num(&first_file_lf, &second_file_lf);
    println!("{}: {}", "Files have same number of rows".green(), row_num);

    let first_file_cols = get_column_names(&first_file_lf);
    let second_file_cols = get_column_names(&second_file_lf);

    assert_both_frames_are_comparable(
        &first_file_cols,
        &second_file_cols,
        args.strict_column_order,
    );
    println!("{}", "Files have comparable columns".green());

    let sorting_column = &first_file_cols[0];
    let columns_to_iterate = (first_file_cols.len() - 1) as u64;

    println!(
        "Comparing content of columns in both files when sorted by column \"{}\"...",
        sorting_column
    );
    let progress_bar = ProgressBar::new(columns_to_iterate);
    progress_bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos:>7}/{len:7} {msg}")
            .expect("Error creating progress bar. Incorrect Style?. Please raise issue to developers of this tool"));

    let number_of_columns_to_compare = args.number_of_columns;
    let mut columns_to_compare = vec![];
    for i in 1..first_file_cols.len() {
        let column_name = &first_file_cols[i];
        columns_to_compare.push(column_name);

        if columns_to_compare.len() == number_of_columns_to_compare
            || i == first_file_cols.len() - 1
        {
            let first_data_frame = get_sorted_data_frame_for_columns(
                &first_file_lf,
                sorting_column,
                &columns_to_compare,
            );

            let second_data_frame = get_sorted_data_frame_for_columns(
                &second_file_lf,
                sorting_column,
                &columns_to_compare,
            );

            if !first_data_frame.equals_missing(&second_data_frame) {
                let column_names = columns_to_compare
                    .iter()
                    .copied()
                    .map(String::as_str)
                    .collect::<Vec<_>>()
                    .join(" | ");

                println!(
                    "{}: {} \n {} \n {}",
                    "FILES ARE DIFFERENT".red(),
                    "Values for column(s)".red(),
                    column_names.red().bold(),
                    "are different".red()
                );

                exit(3);
            }
            progress_bar.inc(columns_to_compare.len() as u64);
            columns_to_compare.clear();
        }
    }
    progress_bar.finish();

    println!(
        "Files {} and {} {} {}",
        first_file_path.bold(),
        second_file_path.bold(),
        "ARE IDENTICAL WHEN SORTED BY COLUMN:".green(),
        sorting_column.green()
    );
}

fn assert_both_frames_have_same_row_num(
    first_lazy_frame: &LazyFrame,
    second_lazy_frame: &LazyFrame,
) -> u32 {
    let first_row_num = get_rows_num(first_lazy_frame);
    let second_row_num = get_rows_num(second_lazy_frame);

    if first_row_num != second_row_num {
        println!(
            "{}: {} {} <> {}",
            "FILES ARE DIFFERENT".red(),
            "Different number of rows".red(),
            first_row_num.to_string(),
            second_row_num.to_string()
        );

        exit(4);
    }

    return first_row_num;
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

fn get_lazy_frame(file_path: &str, delimiter: char) -> LazyFrame {
    LazyCsvReader::new(file_path)
        .has_header(true)
        .with_infer_schema_length(Some(0))
        .with_separator(delimiter as u8)
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

fn get_sorted_data_frame_for_columns(
    lazy_frame: &LazyFrame,
    sorting_by_column: &String,
    columns: &Vec<&String>,
) -> DataFrame {
    let mut all_columns = vec![col(sorting_by_column)];
    for next_column in columns {
        all_columns.push(col(next_column));
    }

    lazy_frame
        .clone()
        .select(all_columns)
        .sort(sorting_by_column, SortOptions::default())
        .collect()
        .expect(format!("Couldn't sort by column {sorting_by_column}",).as_str())
}

fn get_rows_num(lazy_frame: &LazyFrame) -> u32 {
    let first_column_name = get_column_names(&lazy_frame.clone())[0].to_string();
    return lazy_frame
        .clone()
        .select([col(first_column_name.as_str())])
        .collect()
        .expect("Error when counting the rows of the CSV file")
        .shape()
        .0 as u32;
}
