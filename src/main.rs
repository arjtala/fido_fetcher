use anyhow::{Context, Result};
use clap::Parser;
use csv::{ReaderBuilder, WriterBuilder};
use futures::stream::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs::{File, metadata};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn, instrument, span, Level};
use tracing_subscriber::{fmt, EnvFilter};

const USER_AGENT: &str = "facebookexternalhit";

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
	/// Input TSV file
	#[arg(short, long)]
	input: PathBuf,

	/// Output CSV file path
	#[arg(short, long)]
	output: PathBuf,

	/// Number of concurrent requests
	#[arg(short, long, default_value = "10")]
	concurrency: usize,

	/// Request timeout in seconds
	#[arg(short, long, default_value = "30")]
	timeout: u64,

	/// Skip header row in input file
	no_header: bool,

	/// Log level (error, warn, info, debug, trace)
	#[arg(short, long, default_value = "info")]
	log_level: String,
}

#[derive(Debug, Deserialize)]
struct InputRecord {
	url: String,
	text: String,
}

#[derive(Debug, Serialize)]
struct OutputRecord {
	url: String,
	text: String,
	id: u64,
	download_successful: bool,
}

fn init_tracing(log_level: &str) -> Result<()> {
	let filter = match log_level.to_lowercase().as_str() {
		"error" => "error",
		"warn" => "warn",
		"info" => "info",
		"debug" => "debug",
		"trace" => "trace",
		_ => {
			eprintln!("Invalid log level '{log_level}', defaulting to 'info'");
			"info"
		}
	};

	fmt()
		.with_env_filter(EnvFilter::new(filter))
		.with_target(false)
		.with_thread_ids(true)
		.with_file(true)
		.with_line_number(true)
		.init();

	Ok(())
}

fn generate_url_hash(url: &str) -> u64 {
	let mut hasher = Sha256::new();
	hasher.update(url.as_bytes());
	let hash_bytes = hasher.finalize();

	// Convert first 8 bytes to u64
	let mut bytes = [0u8; 8];
	bytes.copy_from_slice(&hash_bytes[0..8]);
	u64::from_be_bytes(bytes)
}

#[instrument(skip(client), fields(url = %url))]
async fn fetch_url(client: &Client, url: &str) -> bool {
	let span = span!(Level::DEBUG, "http_request", url = %url);
	let _enter = span.enter();

	match client.get(url).send().await {
		Ok(response) => {
			let status = response.status();
			let success = status.is_success();
			if success {
				debug!(status = %status, "Request successful");
			} else {
				warn!(status = %status, "Request failed with non-success status");
			}
			success
		}
		Err(e) => {
			warn!(error = %e, "Request failed with error");
			false
		}
	}
}

async fn get_file_size(path: &PathBuf) -> Result<u64> {
	let metadata = metadata(path).await
		.with_context(|| format!("Failed to get metadata for: {}", path.display()))?;
	Ok(metadata.len())
}

async fn stream_input_records(input_path: &PathBuf, no_header: bool) -> Result<impl futures::Stream<Item = Result<(InputRecord, u64)>>> {
	let span = span!(Level::DEBUG, "stream_setup", file = %input_path.display());
	let _enter = span.enter();

	let file = File::open(input_path).await
		.with_context(|| format!("Failed to open input file: {}", input_path.display()))?;
	let reader = BufReader::new(file);
	let mut lines = reader.lines();

	let mut header_line: String = String::from("");
	if !no_header {
		header_line = lines.next_line().await
			.context("Failed to read header")?
			.ok_or_else(|| anyhow::anyhow!("Empty input file"))?;
	}

	let mut bytes_processed = header_line.len() as u64 + 1; // +1 for newline
	debug!(header_size = bytes_processed, "Header line processed");

	Ok(async_stream::stream! {
		let mut line_number = 1;
		while let Some(line) = lines.next_line().await.transpose() {
			line_number += 1;
			match line {
				Ok(line_content) => {
					let line_size = line_content.len() as u64 + 1; // +1 for newline
					bytes_processed += line_size;

					if line_content.trim().is_empty() {
						debug!(line_number, "Skipping empty line");
						continue;
					}

				let mut reader = ReaderBuilder::new()
					.delimiter(b'\t')
					.has_headers(false)
					.from_reader(line_content.as_bytes());

				if let Some(result) = reader.deserialize().next() {
					match result {
						Ok(record) => {
							debug!(line_number, bytes_processed, "Successfully parsed record");
							yield Ok((record, bytes_processed));
						}
						Err(e) => {
							error!(line_number, error = %e, "Failed to parse TSV record");
							yield Err(anyhow::anyhow!("Failed to parse TSV record on line {}: {}", line_number, e));
						},
					}
				} else {
					warn!(line_number, "No data found on line");
				}
			}
				Err(e) => {
					error!(line_number, error = %e, "Failed to read line");
					yield Err(anyhow::anyhow!("Failed to read line {}: {}", line_number, e));
				}
			}
		}
	})
}

#[instrument(skip(input_path, output_path), fields(
	input_file = %input_path.display(),
	output_file = %output_path.display(),
	concurrency = concurrency,
	timeout_secs = timeout_secs
))]
async fn process_records(
	input_path: &PathBuf,
	output_path: &PathBuf,
	concurrency: usize,
	timeout_secs: u64,
	no_header: bool,
) -> Result<()> {
	// Count total records for progress bar
	let total_file_size = get_file_size(input_path).await
		.context("Failed to get input file size")?;

	info!(file_size = total_file_size, concurrency = concurrency, timeout = timeout_secs, "Starting URL processing");

	// Setup progress bar with file size a total
	let pb = ProgressBar::new(total_file_size);
	pb.set_style(
		ProgressStyle::with_template(
			"{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec)} (msg)"
		)
			.unwrap()
			.progress_chars("#>-")
	);
	pb.set_message("Processing URLs...");

	// Setup HTTP client with timeout
	let span = span!(Level::DEBUG, "client_setup");
	let _enter = span.enter();
	let client = Client::builder()
		.timeout(Duration::from_secs(timeout_secs))
		.user_agent(USER_AGENT)
		.build()
		.context("Failed to create HTTP client")?;
	drop(_enter);

	// Setup output file and CSV writer
	let span = span!(Level::DEBUG, "output_setup");
	let _enter = span.enter();
	let output_file = File::create(output_path).await
		.with_context(|| format!("Failed to create output file: {}", output_path.display()))?;
	let mut buf_writer = BufWriter::new(output_file);

	// Write CSV header
	buf_writer.write_all(b"url,text,id,download_successful\n").await
		.context("Failed to write CSV header")?;
	debug!("CSV header written");
	drop(_enter);

	// Create channel for streaming results to writer
	let (tx, mut rx) = mpsc::channel::<OutputRecord>(100);

	// Spawn task to write results as they come in
	let writer_handle = {
		tokio::spawn(async move {
			let span = span!(Level::DEBUG, "writer_task");
			let _enter = span.enter();

			let mut successful_count = 0u64;
			let mut total_count = 0u64;

			while let Some(record) = rx.recv().await {
				total_count += 1;
				if record.download_successful {
					successful_count += 1;
				}

				// Serialize record to CSV
				let mut csv_writer = WriterBuilder::new()
					.has_headers(false)
					.from_writer(Vec::new());

				if let Err(e) = csv_writer.serialize(&record) {
					error!(url = %record.url, error = %e, "Failed to serialize record");
					continue;
				}

				if let Ok(csv_data) = csv_writer.into_inner() {
					if let Err(e) = buf_writer.write_all(&csv_data).await {
						error!(url = %record.url, error = %e, "Failed to write record");
						continue
					}
				}

				// Log progress periodically
				if total_count % 100 == 0 {
					debug!(
						proccessed = total_count,
						successful = successful_count,
						success_rate = (successful_count * 100) / total_count,
						"Progress update"
					);
				}
			}

			// Flush the writer
			if let Err(e) = buf_writer.flush().await {
				error!(error = %e, "Failed to flush output buffer");
			} else {
				debug!("Output buffer flushed successfully");
			}

			info!(
				total_processed = total_count,
				successful = successful_count,
				"Writer task completed"
			);
			(successful_count, total_count)
		})
	};

	// Stream input records and process them continuously
	let input_stream = stream_input_records(input_path, no_header).await?;

	let span = span!(Level::INFO, "concurrency_processing");
	let _enter = span.enter();

	input_stream
		.map(|record_result| {
			let client = client.clone();
			let tx = tx.clone();
			let pb = pb.clone();
			async move {
				match record_result {
					Ok((record, bytes_processed)) => {
						// Update progress bar with bytes processed
						pb.set_position(bytes_processed);

						let url_hash = generate_url_hash(&record.url);
						let download_successful = fetch_url(&client, &record.url).await;

						let output_record = OutputRecord {
							url: record.url,
							text: record.text,
							id: url_hash,
							download_successful,
						};

						if let Err(e) = tx.send(output_record).await {
							error!(error = %e, "Failed to send record to writer");
						}
					}
					Err(e) => {
						error!(error = %e, "Error processing record");
					}
				}
			}
		})
		.buffer_unordered(concurrency)
		.collect::<Vec<_>>()
		.await;

	drop(_enter);

	// Close the channel and wait for writer to finish
	let span = span!(Level::DEBUG, "cleanup");
	let _enter = span.enter();
	drop(tx);
	let (successful_count, total_processed) = writer_handle.await
		.context("Writer tax failed")?;

	pb.finish_with_message("Processing complete!");

	// Print summary
	let failed_count = total_processed - successful_count;
	let success_rate = if total_processed > 0 {
		(successful_count * 100) / total_processed
	} else {
		0
	};
	let failure_rate = if total_processed > 0 {
		(failed_count * 100) / total_processed
	} else {
		0
	};

	info!(
		total_urls = total_processed,
		successful = successful_count,
		failed = failed_count,
		success_rate = success_rate,
		failure_rate = failure_rate,
		output_file = %output_path.display(),
		"📊 Processing Summary"
	);

	Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
	let args = Args::parse();

	// Initialize tracing first
	init_tracing(&args.log_level)?;

	let span = span!(
		Level::INFO, "main",
		input_file = %args.input.display(),
		output_file = %args.output.display(),
		concurrency = args.concurrency,
		timeout = args.timeout,
		log_level = %args.log_level
	);
	let _enter = span.enter();

	info!("Starting FIDO");

	if !args.input.exists() {
		error!(file = %args.input.display(), "Input file does not exist");
		anyhow::bail!("Input file does not exist: {}", args.input.display());
	}

	process_records(&args.input, &args.output, args.concurrency, args.timeout, args.no_header).await?;

	info!("FIDO completed successfully");
	Ok(())
}
