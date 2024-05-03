# SQL to Neo4j Data Pipeline using PySpark

This project provides a PySpark pipeline to load data from a SQL database into a Neo4j graph database, with appropriate partitioning to handle large datasets efficiently.

## Table of Contents

- [SQL to Neo4j Data Pipeline using PySpark](#sql-to-neo4j-data-pipeline-using-pyspark)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Prerequisites](#prerequisites)
  - [Configuration](#configuration)
  - [Usage](#usage)
  - [Explanation](#explanation)
    - [Calculate Data Size and Number of Partitions:](#calculate-data-size-and-number-of-partitions)
    - [Load Data with Partitioning:](#load-data-with-partitioning)
    - [Transform Data:](#transform-data)
    - [Write Data to Neo4j with Partitioning:](#write-data-to-neo4j-with-partitioning)
    - [Repartition Data Before Writing:](#repartition-data-before-writing)
    - [Notes](#notes)

## Overview

The script reads data from the following SQL tables:
- `actors`: Contains information about actors.
- `genre`: Contains information about genres.
- `movie`: Contains information about movies.
- `acted_in`: Contains relationships indicating which actors acted in which movies.
- `awards`: Contains information about awards won by actors for specific movies.

It then writes this data to a Neo4j database, creating nodes for `Actor`, `Genre`, and `Movie`, and relationships for `ACTED_IN` and `AWARDED`.

## Prerequisites

- Apache Spark
- PySpark
- MySQL or another SQL database
- Neo4j
- JDBC driver for your SQL database
- Neo4j Connector for Apache Spark

## Configuration

1. **Spark Session Initialization:**
   Ensure that the `neo4j-connector-apache-spark` package is included in your Spark session initialization.

2. **JDBC URL and Connection Properties:**
   Update the `jdbc_url` and `connection_properties` with your SQL database connection details.

3. **Neo4j Connection:**
   Update the `neo4j_url` and `neo4j_properties` with your Neo4j database connection details.

## Usage

   ```sh
   git clone <repository-url>
   cd <repository-directory>
   spark-submit your_script.py
   ```

## Explanation

### Calculate Data Size and Number of Partitions:

The calculate_partitions function queries the SQL database to get the total row count of the table and calculates the number of partitions based on a predefined partition_size.

### Load Data with Partitioning:

The load_data_with_partitioning function uses the calculated number of partitions, lowerBound, and upperBound to load data from the SQL tables.

### Transform Data:

The script renames columns to match the expected keys in Neo4j.

### Write Data to Neo4j with Partitioning:

The write_to_neo4j function is used to write the data into Neo4j, handling each partition individually to ensure the data is written efficiently.

### Repartition Data Before Writing:

The data is repartitioned using repartition based on the number of partitions determined from the rdd.getNumPartitions() method before writing to Neo4j.

### Notes
 - Ensure that the JDBC driver for your SQL database is available in your Spark environment.
 - Adjust the partition_size in the calculate_partitions function as needed to fit your specific use case.
 - Make sure to replace the placeholders with actual values for your SQL and Neo4j configurations.