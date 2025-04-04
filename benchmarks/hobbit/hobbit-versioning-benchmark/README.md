# The Semantic Publishing Versioning Benchmark (SPVB)

The Semantic Publishing Versioning Benchmark (SPVB) aims to test the ability of versioning systems to efficiently manage evolving Linked Data datasets and queries evaluated across multiple versions of these datasets. It acts like a Benchmark Generator, as it generates both the data and the queries needed to test the performance of the versioning systems. 

The data generator of SPVB uses the data generator of Linked Data Benchmark Council ([LDBC](http://www.ldbcouncil.org/)) Semantic Publishing Benchmark ([SPB](http://ldbcouncil.org/developer/spb)) as well as real [DBpedia](http://wiki.dbpedia.org/) data. SPVB is not tailored to any versioning strategy (the way that versions are stored) and can produce data of different sizes, that can be altered in order to create arbitrary numbers of versions using configurable insertion and deletion ratios. In particular, the following parameters can be set to configure the data generation process:

**Configuration Parameters**:

* **Number of versions** defines the number of versions to produce. The default value is `5`.
* **Initial version size** defines the size of the initial version of the dataset in terms of triples. The default value is `100.000`.
* **Version insertion ratio** defines the proportion of added triples between two consecutive versions. The default value is `15%`.
* **Version deletion ratio** defines the proportion of deleted triples between two consecutive versions. The default value is `10%`.
* **Generated data form**: Since each system implements a different versioning strategy, it requires the generated data in a specific form. SPVB’s data generator can output the data as i) an *Independent Copy* (suitable for systems that implement the full materialization strategy), ii) as a *Changeset* − set of added and deleted triples (suitable for systems implementing the delta-based or annotated triples versioning strategies) or iii) both as an independent copy and changeset (suitable for systems implementing a hybrid strategy). The default value is `Independent Copies (IC)`.
* **Enable/Disable Query Types** let the system under test to determine the query types for which will be tested in.
* **Generator seed** is used to set the random seed for the data generator. This seed is used to control all random data generation happening in SPVB. The default value is `100`.

In [this](https://docs.google.com/spreadsheets/d/1LawP20KYzK7M6KxBdec_eSWAPYx8M9XVeEBaz4n5uLU/edit) online spreadsheet someone could emulate the version to version data evolution in terms of triples number. The viewer is able to edit the yellow cells correspond to the following configuration parameters: i) *Number of versions*, ii) *Initial version size*, iii) *Version insertion ratio* and iv) *Version deletion ratio* and observe how the number of triples changes from one version to another.

The generated SPARQL queries are of different types and are partially based on a subset of the 25 query templates defined in the context of DBpedia SPARQL Benchmark ([DBPSB](http://aksw.org/Projects/DBPSB.html)).


# KPIs

SPVB evaluates the correctness and performance of the system under test through the following Key Performance Indicators (KPIs)

* **Query failures**: The number of queries that failed to execute.
* **Throughput (in queries per second)**: The execution rate per second for all queries.
* **Initial version ingestion speed (in triples per second)**: The total triples that can be loaded per second for the dataset’s initial version.
* **Applied changes speed (in triples per second)**: The average number of changes that can be stored by the benchmarked system per second after the loading of all new versions.
* **Storage space cost (in MB)**: This KPI measures the total storage space required to store all versions.
* **Average query execution time (in ms)**: The average execution time, in milliseconds for each one of the eight versioning query types.

# Running the Benchmark
If you want to run SPVB using the platform, please follow the guidelines found here: https://github.com/hobbit-project/platform/wiki/Experiments

