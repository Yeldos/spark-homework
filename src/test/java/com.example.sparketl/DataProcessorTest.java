package com.example.sparketl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Tests the functionality of the DataProcessor class.
public class DataProcessorTest {
    private static SparkSession spark;

    @BeforeAll
    public static void setup() {
        spark = SparkSession.builder()
            .appName("Spark ETL Test")
            .master("local[*]")
            .getOrCreate();
    }

    @Test
    public void testFilterInvalidLocations() {
        DataProcessor processor = new DataProcessor(spark);
        Dataset<Row> input = spark.read().option("header", "true").csv("test-data/restaurants.csv");
        Dataset<Row> result = processor.filterInvalidLocations(input);

        assertEquals(1, result.count());
    }
}
