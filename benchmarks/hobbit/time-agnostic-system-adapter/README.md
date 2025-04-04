# Time-Agnostic RDF System Adapter for HOBBIT Versioning Benchmark

This project implements a system adapter for the [Semantic Publishing Versioning Benchmark (SPVB)](https://github.com/hobbit-project/versioning-benchmark) to benchmark a time-agnostic RDF query system.

## Overview

This adapter connects your time-agnostic RDF system to the HOBBIT benchmarking platform, allowing it to be evaluated on the following key performance indicators (KPIs):

* Query failures: The number of queries that failed to execute
* Throughput (queries per second): The execution rate per second for all queries
* Initial version ingestion speed (triples per second): Loading speed for the dataset's initial version
* Applied changes speed (triples per second): Average number of changes that can be stored per second
* Storage space cost (in MB): Total storage space required for all versions
* Average query execution time (ms): Average execution time for each of the eight versioning query types

## Requirements

* Java 8 or higher
* Maven 3.6 or higher
* Your time-agnostic RDF query system

## Building the Adapter

The project includes an enhanced build script with multiple options:

```bash
cd time-agnostic-system-adapter
./build.sh [OPTIONS]
```

Available options:

* `-h, --help`: Show help message
* `-c, --clean`: Clean Maven cache before building
* `-d, --deps`: Build required dependencies (hobbit-core) before building this project
* `--docker`: Build Docker image after successful build

Examples:

```bash
./build.sh               # Standard build
./build.sh -c            # Clean Maven cache then build
./build.sh -d            # Build dependencies then this project
./build.sh -cd --docker  # Clean, build dependencies, build project, create Docker image
```

## Configuration

Before using the adapter, you need to customize it to work with your specific time-agnostic RDF system. The key methods to implement are:

1. `startTimeAgnosticSystem()`: Start your system
2. `stopTimeAgnosticSystem()`: Stop your system
3. `loadRdfFileToSystem(File, String)`: Load an RDF file into a specific version in your system
4. `executeQuery(String, ByteArrayOutputStream)`: Execute a SPARQL query and write results as JSON

You can also customize the system configuration in `src/main/resources/config.properties`.

## Integration with HOBBIT Platform

To use this adapter with the HOBBIT platform:

1. Build the project with Docker image: `./build.sh --docker`
2. Upload the image to the HOBBIT platform
3. Configure the benchmark parameters
4. Run the benchmark

## Local Testing

For local testing before deploying to the HOBBIT platform, you can:

1. Clone the [versioning-benchmark](https://github.com/hobbit-project/versioning-benchmark) repository
2. Copy your adapter JAR to the appropriate location
3. Configure the benchmark's parameters in `docker-compose.yml`
4. Run the benchmark using Docker Compose

## License

This project is licensed under the ISC License.

## References

* [HOBBIT Platform](https://github.com/hobbit-project/platform)
* [Semantic Publishing Versioning Benchmark](https://github.com/hobbit-project/versioning-benchmark) 