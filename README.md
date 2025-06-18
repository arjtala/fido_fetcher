URL Fetcher

A Rust command-line tool that processes TSV files containing URLs and text, attempts to fetch each URL, and outputs the results to a CSV file with success indicators.
Features

    Reads TSV files with URL and text columns
    Concurrent HTTP requests with configurable concurrency
    Progress bar showing real-time download progress
    Generates unique 64-bit hash IDs for each URL
    Outputs CSV with success/failure flags
    Configurable request timeout
    Comprehensive error handling

Installation

Make sure you have Rust installed, then build the project:

```bash
cargo build --release
```

Usage

```bash
cargo run -- --input input.tsv --output output.csv
```

Command Line Options
```
    --input, -i: Path to input TSV file (required)
    --output, -o: Path to output CSV file (required)
    --concurrency, -c: Number of concurrent requests (default: 10)
    --timeout, -t: Request timeout in seconds (default: 30)
    --no-header: Skip header row detection (treat first line as data)
```

Example

```bash
# Process a TSV file with custom settings
cargo run -- -i urls.tsv -o results.csv -c 20 -t 60

# Process a file without headers
cargo run -- -i urls.tsv -o results.csv --no-header

# Or using the built binary
./target/release/url-fetcher -i urls.tsv -o results.csv
```

Input Format

The input TSV file should contain at least two tab-separated columns:
```
    First column: URL to fetch
    Second column: Associated text
```

The tool automatically detects whether the first row is a header by looking for common header keywords like "url", "link", "text", "description", etc. If the first column of the first row starts with "http", it's treated as data.
With Header Example (input.tsv):
```tsv
url	text
https://example.com	Example website
https://httpbin.org/status/200	Test endpoint
https://httpbin.org/status/404	This will fail
```

Without Header Example (input.tsv):
```tsv
https://example.com	Example website
https://httpbin.org/status/200	Test endpoint
https://httpbin.org/status/404	This will fail
```
Output Format

The output CSV file contains:
```
    url: Original URL
    text: Original text
    id: 64-bit hash ID generated from the URL
    download_successful: Boolean indicating if the fetch was successful
```
Example output.csv:

```csv
url,text,id,download_successful
https://example.com,Example website,12345678901234567890,true
https://httpbin.org/status/200,Test endpoint,09876543210987654321,true
https://httpbin.org/status/404,This will fail,11111111111111111111,false
``
Dependencies
```
    tokio: Async runtime
    reqwest: HTTP client for fetching URLs
    csv: CSV reading and writing
    serde: Serialization/deserialization
    clap: Command line argument parsing
    indicatif: Progress bar
    anyhow: Error handling
    sha2: SHA-256 hashing for URL IDs
    futures: Stream processing utilities
```

Error Handling

The tool handles various error conditions:
```
    Invalid input file format
    Network timeouts
    HTTP errors
    File I/O errors
    Invalid URLs
``
Failed requests are marked as unsuccessful in the output CSV but don't stop the processing of other URLs.
Performance

The tool uses async/await for concurrent processing, allowing multiple URLs to be fetched simultaneously. The default concurrency is 10, but this can be adjusted based on your needs and the target servers' capacity.
License

This project is available under the MIT License.
