# Transactional Cloud Serving Benchmark (TCSB)

## Welcome!

The TCSB is a benchmarking system for comparing the performance of databases under specific workloads.

These workloads model "transactional cloud serving" use cases.

We use the phrase "transactional cloud serving" to describe scenarios in which traditional OLTP workloads are executed on databases using distributed transactions.

For example, building a modern e-commerce site may necessitate creating a globally-distributed and failure-resilient shopping cart and checkout system. To implement this, you need a database that permits you to make all-or-nothing transactions, while maintaining integrity in the face of heavy load and major outages.

## User Guide

This benchmark suite is made up of a set of independent tools, which may be combined using `stdio` according to the Unix philosophy.

### Full example

Please see the file `example_data/full_example.log` for a full example of using the benchmark suite to test Mongo.

### Setup: Download code

```
git clone https://github.com/transactional-cloud-serving-benchmark/tcsb
```

### Setup: Build executables

Make sure you've installed Go, set up your GOPATH for module usage, and execute:

```
./build_executables.sh
```

This uses Go to build all commands in the `./cmd` directory, and stores the resulting binaries in `./artifacts`.

```
$ ls artifacts/
check_validation_results
generate_commands
gold_memory_client
gold_memory_server
mongo_client
```

In the near future, when we use other languages to implement database clients, these instructions will be updated.

### Setup: Debugging

This project was developed using Go version 1.12.1, using the recently-added Go module system.

If you are finding errors involving Go dependency management, or the `GOPATH` environment variable, please read the following guide to set up your development environment:

https://blog.golang.org/modules2019


### Tool: `generate_commands`

Use this tool to generate commands for a particular command driver.

Parameters include:

+ Random number generator seed
+ Scenario type
+ Scenario parameters
+ Write batch size


#### Example: Generate human-readable output (for debugging)

The following command generates 10 sorted key/value commands, with a write ratio of 44%, batch_size of 2, and prints them to stdout for debugging.

```
$ ./artifacts/generate_commands keyvalue gold_memory exec debug seed=4321,write_ratio=0.44,command_count=10,key_ordering=sorted,key_len=10,val_len=10,write_batch_size=2
0 0 0 w aaaaaaaaaa zomvtmqeym
1 0 1 w aaaaaaaaab bomzplglwh
2 1 0 r aaaaaaaaaa
3 2 0 r aaaaaaaaaa
4 3 0 r aaaaaaaaab
5 4 0 w aaaaaaaaac oqfowotylo
6 4 1 w aaaaaaaaad acjnyrzcem
7 5 0 r aaaaaaaaac
8 6 0 r aaaaaaaaac
9 7 0 r aaaaaaaaaa
```

Each line has the following format:
```
<command id> <batch id> <command-in-batch id> <command type> <key> [value]
```

By inspection, we can see that:

+ 10 total operations were generated (first column).
+ 8 total batches were generated (second column).
+ 2 write batchs were generated, each with 2 write commands (fourth column).
+ 6 read batches were generated, each with 1 read command (fourth column).

Furthermore,

+ Each read command refers only to a previously-written key. For example, commands `2,3,4` only refer to `aaaaaaaaaa` and `aaaaaaaaab`.
+ The actual write ratio was 40%, not 44%, because the given parameters prevented meeting that target exactly. To meet ratio targets, increase the command count or decrease the batch size.

#### Example: Generate binary output (for benchmarking)

This command generates 1000 random key/value commands, with a write ratio of 33%, batch_size of 10, and serializes them to stdout as length-delimited FlatBuffers objects.

We use the flag `binary` to do this.

Note that this data is not human-readable.

```
$ ./artifacts/generate_commands keyvalue gold_memory exec binary seed=4321,write_ratio=0.44,command_count=1000,key_ordering=sorted,key_len=10,val_len=10,write_batch_size=2 > commands.dat
$ wc -c commands.dat
94780 commands.dat
```

We can see that the resulting data is approximately 95K in size, which is about 5x the size of the underlying, meaninful data.

You can expect the overhead per command to go down as the key length, value length, or batch sizes increase.

### Tools: `<database_type>_client`

Use these tools to execute the commands generated by `generate_commands` against the specified database.

The input must come from the command generation's binary mode.

These tools are special-purpose for each database, to take advantage of the speed advantages of some client libraries over others. For example, while the TCSB is written primarily in Go, some drivers may be written in other languages to take advantage of better client implementations.

Parameters may include:

+ Parallelism
+ Batch size
+ Cluster hostnames
+ Generic or scenario-specific database configurations, such as schema definitions
+ Whether to save the results of a set of commands for result validation

### Tool: `gold_standard_dummy_server`

Use this simple in-memory datastore as a baseline for command correctness. This tool is mainly useful to the developers who work on the TCSB itself.

### Tool: `check_validation_results`

Use this tool to validate command results against each other. This is our end-to-end test to check that the scenarios, and database drivers, are implemented correctly.

## Developer Guide

### FlatBuffers schema and code generation

We use FlatBuffers for serializing commands to the clients.

You can regenerate the code for your schema using this command:

```
flatc --go messages.fbs
```
