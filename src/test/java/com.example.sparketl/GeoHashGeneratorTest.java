package com.example.sparketl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Tests the GeoHashGenerator class
class GeoHashGeneratorTest {
    @Test
    void testGeoHashGeneration() {
        SparkSession spark = SparkSession.builder()
            .appName("GeoHashGeneratorTest")
            .master("local[*]")
            .getOrCreate();

        var input = spark.createDataFrame(
            List.of(
                RowFactory.create(20.763, -156.458)
            ),
            new StructType()
                .add("lat", DataTypes.DoubleType)
                .add("lng", DataTypes.DoubleType)
        );

        GeoHashGenerator generator = new GeoHashGenerator();
        Dataset<Row> result = generator.addGeoHash(input);

        result.show();
        assertEquals("8e8w", result.collectAsList().get(0).getAs("geohash"));
        spark.stop();
    }
}
