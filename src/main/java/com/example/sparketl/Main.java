package com.example.sparketl;

import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {
        // Create a SparkSession for processing data
        SparkSession spark = SparkSession.builder()
            .appName("Spark ETL Project") // Application name
            .master("local[*]") // Run locally with available cores
            .getOrCreate();

        // Define file paths for restaurant and weather data
        String restaurantPath = "data/restaurant";
        String weatherPath = "data/weather";
        String outputPath = "output/processed-restaurant-data";

        // Initialize the different components needed for the ETL process
        DataProcessor dataProcessor = new DataProcessor(spark);
        GeoLocationUpdater locationUpdater = new GeoLocationUpdater();
        GeoHashGenerator geoHashGenerator = new GeoHashGenerator();
        DataJoiner dataJoiner = new DataJoiner();
        ParquetWriter parquetWriter = new ParquetWriter();

        // Load the restaurant data and filter for valid and invalid locations.
        var restaurants = dataProcessor.loadRestaurantData(restaurantPath);
        var validRestaurants = dataProcessor.filterValidLocations(restaurants);
        var invalidRestaurants = dataProcessor.filterInvalidLocations(restaurants);

        // Update the geolocation for rows with null lat or lng
        var updatedInvalidRestaurants = locationUpdater.updateNullGeoLocations(spark, invalidRestaurants);
        // Combine valid and updated invalid restaurant data
        var updatedRestaurants = validRestaurants.union(updatedInvalidRestaurants);

        // Load the weather data
        var weatherData = dataProcessor.loadWeatherData(weatherPath);

        // Generate geohashes for restaurant and weather datasets.
        var geohashRestaurants = geoHashGenerator.addGeoHash(updatedRestaurants);
        var geohashWeather = geoHashGenerator.addGeoHash(weatherData);

        // Join the weather and restaurant datasets
        var enrichedData = dataJoiner.joinWeatherAndRestaurants(geohashRestaurants, geohashWeather);

        // Save the enriched dataset to the output path in Parquet format
        parquetWriter.saveToParquet(enrichedData, outputPath);

        // Stop the SparkSession.
        spark.stop();
    }
}
