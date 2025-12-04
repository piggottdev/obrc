# 1️⃣🐝🏎️ The One Billion Row Challenge

The One Billion Row Challenge (1BRC) is a fun exploration of how far modern Java can be pushed for aggregating one billion rows from a text file.
Grab all your (virtual) threads, reach out to SIMD, optimize your GC, or pull any other trick, and create the fastest implementation for solving this task!

Challenge repo: https://github.com/gunnarmorling/1brc

## Results

CI Results are from 500_000_000 row runs.

<!-- RESULTS_START -->

| Commit | Message | Runtime | Difference | Improvement |
|--------|---------|---------|------------|-------------|
| 7de0059 | Baseline | 99.066s | 0.000s | 0.00% |
| cab56e0 | Multi-threaded | 25.545s | -70.602s | 73.43% |
| 94f84ac | Custom Float Parsing | 12.888s | -77.123s | 85.68% |
| 2ab3fe9 | Absolute Buffer Operations | 14.500s | -73.144s | 83.46% |
| 3aa9ba7 | Relative Buffer Operation | 13.156s | -77.239s | 85.45% |

<!-- RESULTS_END -->

## The Challenge

The text file contains temperature values for a range of weather stations.
Each row is one measurement in the format `<string: station name>;<double: measurement>`, with the measurement value having exactly one fractional digit.
The following shows ten rows as an example:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
```

The task is to write a Java program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like this
(i.e. sorted alphabetically by station name, and the result values per station in the format `<min>/<mean>/<max>`, rounded to one fractional digit):

```
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
```

## Prerequisites

- [Java 21](https://openjdk.org/projects/jdk/21/) must be installed on your system.
- [Maven](https://maven.apache.org/) installed to run mvn commands - not required to run.

## Running the Challenge

The solution implementation class can be found at [`dev.pig.obrc.CalculateAverage`](src/main/java/dev/pig/obrc/CalculateAverage.java). 
Solutions should be placed inside the `run` function, without editing the function signature.
The function should return the expected output as a string.

The [`dev.pig.obrc.Benchmark`](src/main/java/dev/pig/obrc/Benchmark.java) class has been provided for solution validation and benchmarking.
It will:
- Generate an input file if not present at [`measurements.txt`](measurements.txt).
- Run a baseline benchmark if expected output file not present at [`results_baseline.out`](results_baseline.out).
- Run solution benchmark, generates output file for inspection at [`results.out`](results.out).
- Compares output files to validate solution correctness.

This can also be run using the convenience Maven script:
```bash
mvn clean install
```

### CI

A [GitHub Actions Pipeline](.github/workflows/benchmark.yaml) and Java [Runner](src/main/java/dev/pig/obrc/pipeline/Runner.java) have been provided to allow for easy results tracking in a repo.
On push to main, the benchmark runner will run the same steps as the local pipeline and also automatically update [results CSV](results.csv) and the results table in the [readme](README.md).

