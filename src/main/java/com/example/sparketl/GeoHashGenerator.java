package com.example.sparketl;

import ch.hsr.geohash.GeoHash;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

// Adds a geohash column to a dataset based on lat and lng
public class GeoHashGenerator {
    public Dataset<Row> addGeoHash(Dataset<Row> dataset) {
        // Create a new column 'geohash' using a user-defined function (UDF) to generate geohash values based on latitude and longitude
        return dataset.withColumn("geohash", functions.udf(
            (Double lat, Double lng) -> {
                // Check if latitude and longitude are not null before generating geohash
                if (lat != null && lng != null) {
                    // Generate a geohash with a precision of 4 characters
                    return GeoHash.withCharacterPrecision(lat, lng, 4).toBase32();
                }
                // Return null if latitude or longitude is null
                return null;
            },
            DataTypes.StringType // Define the return type as String for the UDF
        ).apply(functions.col("lat"), functions.col("lng"))); // Apply the UDF to the 'lat' and 'lng' columns of the dataset
    }
}
