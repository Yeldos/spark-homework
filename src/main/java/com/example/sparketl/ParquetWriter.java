package com.example.sparketl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

// Responsible for writing data to a Parquet file
public class ParquetWriter {
    public void saveToParquet(Dataset<Row> dataset, String outputPath) {
        // Check if the dataset has columns
        if (dataset.columns().length > 0) {
            // Write the dataset to the specified path with SaveMode.Overwrite and partitioned by year, month, and day.
            dataset.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("year", "month", "day")
                .parquet(outputPath);
        } else {
            System.out.println("Empty dataset. Skipping write operation.");
        }
    }
}
