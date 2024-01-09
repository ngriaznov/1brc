use hashbrown::HashMap;
use memmap::MmapOptions;
use rayon::prelude::*;
use std::cmp::{max, min};
use std::fs::File;
use std::io;
use std::time::Instant;

// StationData holds temperature data for a station.
#[derive(Clone)]
struct StationData {
    min_temp: i8,
    max_temp: i8,
    total_temp: i8,
    count: i8,
}

impl StationData {
    // Constructor for StationData.
    fn new() -> Self {
        StationData {
            min_temp: i8::MAX,
            max_temp: i8::MIN,
            total_temp: 0,
            count: 0,
        }
    }

    // Updates the StationData with a new temperature reading.
    fn update(&mut self, temp: i8) {
        self.min_temp = min(self.min_temp, temp);
        self.max_temp = max(self.max_temp, temp);
        self.total_temp += temp;
        self.count += 1;
    }

    // Aggregates data from another StationData instance.
    fn aggregate(&mut self, other: &StationData) {
        self.min_temp = min(self.min_temp, other.min_temp);
        self.max_temp = max(self.max_temp, other.max_temp);
        self.total_temp += other.total_temp;
        self.count += other.count;
    }
}

fn main() -> io::Result<()> {
    let start = Instant::now();

    // Load and map the file into memory for fast access.
    let file = File::open("measurements.txt")?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let content = unsafe { std::str::from_utf8_unchecked(&mmap) };

    // Process data in parallel using Rayon.
    let estimated_unique_stations = 10000;
    let aggregated_results: HashMap<String, StationData> = content
        .par_lines()
        .fold(
            || HashMap::with_capacity(estimated_unique_stations),
            process_line,
        )
        .reduce(HashMap::new, |mut acc, h| {
            for (station, data) in h {
                acc.entry(station)
                    .and_modify(|e| e.aggregate(&data))
                    .or_insert(data);
            }
            acc
        });

    // Format results for output.
    let mut formatted_results: Vec<_> = aggregated_results
        .into_iter()
        .map(|(name, data)| {
            let mean = (data.total_temp as f32) / (data.count as f32 * 10.0);
            (
                name,
                format!(
                    "{:.1}/{:.1}/{:.1}",
                    data.min_temp as f32 / 10.0,
                    mean,
                    data.max_temp as f32 / 10.0
                ),
            )
        })
        .collect();

    // Efficient string concatenation for output.
    let mut output_result = String::with_capacity(estimated_unique_stations * 50);
    output_result.push('{');
    formatted_results.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    for (i, (station, result)) in formatted_results.iter().enumerate() {
        let temp_result = format!("{}{}={}", if i > 0 { ", " } else { "" }, station, result);
        output_result += &temp_result;
    }
    output_result.push('}');
    output_result.push('\n');

    // Display results.
    println!("{}", output_result);

    // Report time taken for processing.
    let duration = start.elapsed();
    println!("Time elapsed is: {:?}", duration);

    Ok(())
}

// Process a single line of input data.
fn process_line(mut acc: HashMap<String, StationData>, line: &str) -> HashMap<String, StationData> {
    let (station, temp_str) = split_once(line, b';');
    let temp = parse_temperature(temp_str);

    acc.entry(station.to_string())
        .and_modify(|entry| entry.update(temp))
        .or_insert_with(|| {
            let mut data = StationData::new();
            data.update(temp);
            data
        });

    acc
}

// Splits a string once based on a given delimiter.
fn split_once(input: &str, delimiter: u8) -> (&str, &str) {
    let bytes = input.as_bytes();
    if let Some(pos) = bytes.iter().position(|&b| b == delimiter) {
        (&input[..pos], &input[pos + 1..])
    } else {
        (input, "")
    }
}

// Parses a temperature value from a string.
fn parse_temperature(temp_str: &str) -> i8 {
    let bytes = temp_str.as_bytes();
    let mut temp = 0i8;
    let mut negative = false;
    let mut decimal_found = false;

    for &byte in bytes {
        match byte {
            b'-' => negative = true,
            b'.' => decimal_found = true,
            _ if byte.is_ascii_digit() => {
                temp = temp * 10 + (byte - b'0') as i8;
                if decimal_found {
                    decimal_found = false;
                }
            }
            _ => {}
        }
    }

    if negative {
        -temp
    } else {
        temp
    }
}
