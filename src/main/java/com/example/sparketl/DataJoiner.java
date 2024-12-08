package com.example.sparketl;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

// Responsible for joining two datasets: restaurants and weather data
public class DataJoiner {
    public Dataset<Row> joinWeatherAndRestaurants(Dataset<Row> restaurants, Dataset<Row> weather) {
        // Rename duplicate columns in the weather dataset before joining to prevent column name conflicts
        Dataset<Row> renamedWeather = weather
            .withColumnRenamed("lat", "weather_lat")
            .withColumnRenamed("lng", "weather_lng");

        // Perform a left outer join on restaurants and renamedWeather using the geohash column as the key
        Dataset<Row> joinedData = restaurants.join(renamedWeather, "geohash", "left_outer");

        // Return the joined dataset, which now contains data from both restaurant and weather datasets
        return joinedData;
    }
}
