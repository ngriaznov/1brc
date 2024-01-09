use hashbrown::HashMap;
use memmap::MmapOptions;
use rayon::prelude::*;
use std::fs::File;
use std::io;
use std::str::FromStr;
use std::time::Instant;

#[derive(Clone)]
struct StationData {
    min_temp: f32,
    max_temp: f32,
    total_temp: f32,
    count: i32,
}

impl StationData {
    fn new() -> Self {
        StationData {
            min_temp: f32::MAX,
            max_temp: f32::MIN,
            total_temp: 0.0,
            count: 0,
        }
    }

    fn update(&mut self, temp: f32) {
        self.min_temp = f32::min(self.min_temp, temp);
        self.max_temp = f32::max(self.max_temp, temp);
        self.total_temp += temp;
        self.count += 1;
    }

    fn aggregate(&mut self, other: &StationData) {
        self.min_temp = f32::min(self.min_temp, other.min_temp);
        self.max_temp = f32::max(self.max_temp, other.max_temp);
        self.total_temp += other.total_temp;
        self.count += other.count;
    }
}

fn main() -> io::Result<()> {
    let start = Instant::now();

    // Load and map the file into memory for fast access.
    let file = File::open("C:\\BRC\\1brc\\measurements.txt")?;
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
            let mean = data.total_temp / data.count as f32;
            (
                name,
                format!(
                    "{:.1}/{:.1}/{:.1}",
                    data.min_temp,
                    mean,
                    data.max_temp
                ),
            )
        })
        .collect();

    // Concatenate resulting string
    let mut output_result = String::with_capacity(estimated_unique_stations * 50);
    output_result.push('{');
    formatted_results.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    for (i, (station, result)) in formatted_results.iter().enumerate() {
        let temp_result = format!("{}{}={}", if i > 0 { ", " } else { "" }, station, result);
        output_result += &temp_result;
    }
    output_result.push('}');
    output_result.push('\n');

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
fn parse_temperature(temp_str: &str) -> f32 {
    f32::from_str(temp_str).unwrap()
}
