# ETL Pipeline for Restaurant and Weather Data

## Project Overview

This project implements an **ETL (Extract, Transform, Load)** pipeline that processes restaurant and weather data. The data is stored in CSV and Parquet formats, and the output is saved in Parquet format for easy consumption. The pipeline integrates data enrichment by adding geohash information and using the **OpenCage API** to update missing geolocation data.

## Directory Structure

```plaintext
project-root/
├── data/
│   ├── restaurant/        # Raw restaurant data in CSV format
│   └── weather/           # Raw weather data in Parquet format
├── output/
│   └── processed-restaurant-data/  # Processed data output in Parquet format
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com.example.sparketl/
│   │   │       ├── DataProcessor.java     # Handles data loading and filtering
│   │   │       ├── GeoHashGenerator.java  # Generates geohash for data enrichment
│   │   │       ├── GeoLocationUpdater.java # Uses OpenCage API to update geolocation data
│   │   │       ├── DataJoiner.java         # Joins restaurant and weather data
│   │   │       ├── Main.java               # Main class to run the pipeline
│   │   │       └── ParquetWriter.java      # Writes output data in Parquet format
│   │   └── resources/
│   │       └── .env                        # Stores OpenCage API key securely
│   └── test/
│       └── java/
│           └── com.example.sparketl/
│               ├── DataProcessorTest.java  # Unit tests for DataProcessor class
│               └── GeoHashGeneratorTest.java # Unit tests for GeoHashGenerator class
├── .env                             # Configuration file for environment variables
├── docker-compose.yml               # Docker Compose configuration for running Spark
└── README.md                        # Project documentation
