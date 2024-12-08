package com.example.sparketl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

// Handles data loading and data filtering operations
public class DataProcessor {
    // Spark session to handle data processing tasks
    private final SparkSession spark;

    // Constructor to initialize the DataProcessor with the provided Spark session
    public DataProcessor(SparkSession spark) {
        this.spark = spark;
    }

    // Method to load restaurant data from a CSV file
    public Dataset<Row> loadRestaurantData(String path) {
        return spark.read()
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path);
    }

    // Method to filter out rows with invalid location data (null latitude or longitude)
    public Dataset<Row> filterInvalidLocations(Dataset<Row> restaurants) {
        return restaurants.filter(functions.col("lat").isNull().or(functions.col("lng").isNull()));
    }

    // Method to filter out rows with valid location data (non-null latitude and longitude)
    public Dataset<Row> filterValidLocations(Dataset<Row> restaurants) {
        return restaurants.filter(functions.col("lat").isNotNull().and(functions.col("lng").isNotNull()));
    }

    // Method to load weather data from a Parquet file
    public Dataset<Row> loadWeatherData(String path) {
        return spark.read().parquet(path);
    }
}
