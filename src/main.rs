extern crate byteorder;
extern crate csv;
extern crate datetime;
extern  crate thiserror;

use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use byteorder::{ReadBytesExt, LittleEndian};
use csv::{QuoteStyle, Writer, WriterBuilder};
use datetime::{Duration, Instant, ISO, LocalDate, LocalDateTime, LocalTime, Month};
use thiserror::Error;


struct Bounds {
    digital_min: f32,
    digital_max: f32,
    physical_min: f32,
    physical_max: f32
}
impl Bounds {
    fn scale(&self, &value: &i16) -> Option<f32> {
        if value == i16::MIN {
            return None;
        }
        let value: f32 = value as f32;
        let digital_range: f32 = self.digital_max - self.digital_min;
        let physical_range: f32 = self.physical_max - self.physical_min;

        Some(((value - self.digital_min) * physical_range / digital_range) + self.physical_min)
    }
}


struct Signal {
    label: String,
    dimension: String,
    bounds: Bounds,
    num_samples: usize
}
 

#[derive(Error, Debug)]
enum EdfError {
    #[error("Can't perform csv operation.")]
    Csv(String),
    #[error("Can't parse value to float.")]
    ParseFloat(String),
    #[error("Can't parse value to int.")]
    ParseInt(String),
    #[error("Can't perform I/O operation.")]
    Io(String),
    #[error("Can't parse datetime.")]
    Datetime(String),
    #[error("Number of signals in each sample don't match!")]
    MismatchedSignals(String)



}

impl std::convert::From<csv::Error> for EdfError {
    fn from(err: csv::Error) -> Self {
        EdfError::Csv(err.to_string())
    }
}

impl std::convert::From<std::io::Error> for EdfError {
    fn from(err: std::io::Error) -> Self {
        EdfError::Io(err.to_string())
    }
}

impl std::convert::From<std::num::ParseFloatError> for EdfError {
    fn from(err: std::num::ParseFloatError) -> Self {
        EdfError::ParseFloat(err.to_string())
    }
}

impl std::convert::From<std::num::ParseIntError> for EdfError {
    fn from(err: std::num::ParseIntError) -> Self {
        EdfError::ParseInt(err.to_string())
    }
}


fn get_start_date(reader: &mut BufReader<File>) -> Result<LocalDate, EdfError> {
    let skip_bytes: i64 = 168;
    reader.by_ref().seek_relative(skip_bytes)?;

    let mut day_string = String::with_capacity(2);
    reader.by_ref().take(2).read_to_string(&mut day_string)?;
    let day: i8 = day_string.parse()?;

    reader.by_ref().seek_relative(1)?;

    let mut month_string = String::with_capacity(2);
    reader.by_ref().take(2).read_to_string(&mut month_string)?;
    let month: Month = match Month::from_one(month_string.parse::<i8>()?) {
        Ok(month) => month,
        Err(e) => return Err(EdfError::Datetime(e.to_string()))
    };

    reader.by_ref().seek_relative(1)?;

    let mut year_string = String::with_capacity(2);
    reader.by_ref().take(2).read_to_string(&mut year_string)?;
    let year: i64 = 2000 + year_string.parse::<i64>()?;

    match LocalDate::ymd(year, month, day) {
        Ok(date) => Ok(date),
        Err(e) => Err(EdfError::Datetime(e.to_string()))
    }
}


fn get_start_time(reader: &mut BufReader<File>) -> Result<LocalTime, EdfError> {
    let mut hour_string = String::with_capacity(2);
    reader.by_ref().take(2).read_to_string(&mut hour_string)?;
    let hour: i8 = hour_string.parse()?;

    reader.by_ref().seek_relative(1)?; 

    let mut minute_string = String::with_capacity(2);
    reader.by_ref().take(2).read_to_string(&mut minute_string)?;
    let minute: i8 = minute_string.parse()?;

    reader.by_ref().seek_relative(1)?; 

    let mut second_string = String::with_capacity(2);
    reader.by_ref().take(2).read_to_string(&mut second_string)?;
    let second: i8 = second_string.parse()?;

    match LocalTime::hms(hour, minute, second) {
        Ok(time) => Ok(time),
        Err(e) => Err(EdfError::Datetime(e.to_string()))
    }

}


fn get_num_records(reader: &mut BufReader<File>) -> Result<usize , EdfError> {
    let skip_bytes: i64 = 52;
    reader.by_ref().seek_relative(skip_bytes)?;

    let mut num_records: String = String::with_capacity(8);
    reader.by_ref().take(8).read_to_string(&mut num_records)?;
    Ok(num_records.trim().parse()?)
}


fn get_record_duration(reader: &mut BufReader<File>) -> Result<usize , EdfError> {
    let mut record_duration: String = String::with_capacity(8);
    reader.by_ref().take(8).read_to_string(&mut record_duration)?;
    Ok(record_duration.trim().parse()?)
}


fn get_num_signals(reader: &mut BufReader<File>) -> Result<usize , EdfError> {
    let mut num_signals: String = String::with_capacity(4);
    reader.by_ref().take(4).read_to_string(&mut num_signals)?;
    Ok(num_signals.trim().parse()?)
}


fn get_signals(reader: &mut BufReader<File>, num_signals: usize) -> Result<Vec<Signal>, EdfError> {
    let header_signal_bytes: [u64; 10] = [16, 80, 8, 8, 8, 8, 8, 80, 8, 32];
    let skip_indices: [usize; 3] = [1, 7, 9];

    let s: Vec<String> = Vec::with_capacity(7);
    let mut signals_vec: Vec<Vec<String>> = vec![s; num_signals];
    
    for (i, bytes) in header_signal_bytes.iter().enumerate() {
        let bytes: u64 = *bytes;
        for s in signals_vec.iter_mut() {
            if skip_indices.contains(&i) {
                reader.by_ref().seek_relative(bytes as i64)?;
            } else {
                let mut buf = String::with_capacity(bytes as usize);
                reader.by_ref().take(bytes).read_to_string(&mut buf)?;
                s.push(buf.trim().to_string());
            }
        }
    }
    
    
    let mut signals: Vec<Signal> = Vec::with_capacity(num_signals);
    for s in signals_vec {
        let num_samples: usize = s[6].parse()?;
        signals.push(Signal {
            label: s[0].clone(),
            dimension: s[1].clone(),
            bounds: Bounds {
                physical_min: s[2].parse()?,
                physical_max: s[3].parse()?,
                digital_min: s[4].parse()?,
                digital_max: s[5].parse()?
            },
            num_samples
        })
    }

    Ok(signals)
}


fn read_record_samples(reader: &mut BufReader<File>, num_signals: usize, num_samples: usize) -> Result<Vec<i16>, EdfError> {
    let capacity: usize = num_signals * num_samples;
    let mut values: Vec<i16> = Vec::with_capacity(capacity);
    for _ in 0..capacity {
        let value: i16 = reader.by_ref().read_i16::<LittleEndian>()?;
        values.push(value);
    }
    Ok(values)
}

fn increment_timestamp(mut timestamp: Instant, interval: Duration) -> Instant {
    timestamp = timestamp + interval;
    if timestamp.milliseconds() >= 1000 {
        let seconds: i64 = timestamp.seconds() + (timestamp.milliseconds() / 1000) as i64;
        let milliseconds = timestamp.milliseconds() % 1000;
        timestamp = Instant::at_ms(seconds, milliseconds);
    }
    timestamp
}

fn parse_edf(file_path: &mut PathBuf, target_dir: &Path) -> Result<(), EdfError> {
    let f: File = File::open(&file_path)?;
    let mut reader: BufReader<File> = BufReader::new(f);

    let date: LocalDate = get_start_date(&mut reader)?;
    let time: LocalTime = get_start_time(&mut reader)?;
    let mut timestamp: Instant = LocalDateTime::new(date, time).to_instant();

    let num_records: usize = get_num_records(&mut reader)?;
    let record_duration: usize = get_record_duration(&mut reader)?;
    let num_signals: usize = get_num_signals(&mut reader)?;
    let signals: Vec<Signal> = get_signals(&mut reader, num_signals)?;

    let num_samples:usize = signals[0].num_samples;
    if !&signals.iter().skip(1).map(|s| s.num_samples).all(|n| n == num_samples) {
        let message: String = format!("{}: Not all signals have the same number of samples per record!", &file_path.to_string_lossy());
        return Err(EdfError::MismatchedSignals(message));
    }

    let interval_ms: i16 = (1000.0 * record_duration as f32 / num_samples as f32) as i16;
    let sample_interval: Duration = Duration::of_ms((&interval_ms / 1000) as i64, &interval_ms % 1000);

    file_path.set_extension("csv");
    let target_file: &Path = Path::new(file_path.file_name().unwrap());
    let target_path: PathBuf = target_dir.join(target_file);

    let mut writer: Writer<File> = Writer::from_path(target_path)?;
    let mut row: Vec<String> = Vec::with_capacity(1 + num_signals);

    row.push("timestamp".to_string());
    for signal in &signals {
        row.push(signal.label.clone());
    }
    writer.write_record(&row)?;
    row.clear();

    row.push("YYYY-MM-DD hh:mm:ss".to_string());
    for signal in &signals {
        row.push(signal.dimension.clone());
    }
    writer.write_record(&row)?;

    for _ in 0..num_records {
        let values: Vec<i16> = read_record_samples(&mut reader, num_signals, num_samples)?;
        for i in 0..num_samples {
            row.clear();
            row.push(LocalDateTime::from_instant(timestamp).iso().to_string());

            for j in 0..num_signals {
                let val: &i16 = &values[i + j * num_samples];
                let cleaned_val: String = match signals[j].bounds.scale(val) {
                    Some(scaled) => scaled.to_string(),
                    None => "".to_string()
                };
                row.push(cleaned_val);
            }
            writer.write_record(&row)?;

            timestamp = increment_timestamp(timestamp, sample_interval);
        }
    }
    Ok(())
}


fn is_edf_file(file_path: &Path) -> bool {
    file_path.is_file() && file_path.extension().unwrap() == "edf"
}


fn list_edf_files(dir_path: &PathBuf) -> Vec<PathBuf> {
    let mut edf_list: Vec<PathBuf> = Vec::new();

    let dir_contents = fs::read_dir(dir_path)
        .unwrap()
        .map(|e| e.unwrap().path());

    for file_path in dir_contents {
        if is_edf_file(&file_path) {
            edf_list.push(file_path)
        } else if file_path.is_dir() {
            edf_list.extend(list_edf_files(&file_path))
        }
    }
    edf_list
}


fn get_status_logger() -> Writer<File> {
    let status_file = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open("status.txt")
        .unwrap();
    
    WriterBuilder::new()
        .delimiter(b':')
        .quote_style(QuoteStyle::Always)
        .from_writer(status_file)
}


fn main() {
    let target_dir: &Path = Path::new("./edf_to_csv_files/");
    fs::create_dir_all(target_dir).unwrap();


    let mut edf_file_paths: Vec<PathBuf> = Vec::new();
    for arg in env::args().skip(1) {
        let file_path: PathBuf = PathBuf::from(&arg);
        if is_edf_file(&file_path) {
            edf_file_paths.push(file_path)
        } else if file_path.is_dir() {
            edf_file_paths.extend(list_edf_files(&file_path))
        }
    }
    
    let mut status_logger: Writer<File> = get_status_logger();

    for mut file_path in edf_file_paths {
        match parse_edf(&mut file_path, target_dir) {
            Ok(()) => status_logger.write_record([&LocalDateTime::now().iso().to_string(), file_path.to_str().unwrap(), "File parsed successfully!"]).unwrap(),
            Err(e) => status_logger.write_record([&LocalDateTime::now().iso().to_string(), file_path.to_str().unwrap(), &e.to_string()]).unwrap()
        }
    }
}