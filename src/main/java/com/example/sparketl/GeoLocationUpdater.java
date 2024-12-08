package com.example.sparketl;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.StructType;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Optional;

// Updates null lat and lng values using an external API (OpenCageData)
public class GeoLocationUpdater implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(GeoLocationUpdater.class);
    private static final Dotenv dotenv = Dotenv.configure().load(); // Load environment variables
    private static final String OPEN_CAGE_API_KEY = dotenv.get("OPEN_CAGE_API_KEY"); // Retrieve the API key
    private static final String OPEN_CAGE_API_URL = "https://api.opencagedata.com/geocode/v1/json"; // API URL for geolocation data

    // Method to update rows with null geolocation data by calling an external API
    public Dataset<Row> updateNullGeoLocations(SparkSession spark, Dataset<Row> dataset) {
        StructType schema = dataset.schema(); // Get the schema of the dataset

        // Map function to transform each row and update null latitude and longitude with fetched data
        return dataset.map((MapFunction<Row, Row>) row -> {
            // Extract latitude, longitude, city, and country from the current row
            Double lat = row.isNullAt(row.fieldIndex("lat")) ? null : row.getAs("lat");
            Double lng = row.isNullAt(row.fieldIndex("lng")) ? null : row.getAs("lng");
            String city = row.getAs("city");
            String country = row.getAs("country");

            // If latitude or longitude is null, attempt to fetch the geolocation using an API
            if (lat == null || lng == null) {
                Optional<double[]> geoLocation = fetchGeoLocationFromApi(city, country);
                if (geoLocation.isPresent()) {
                    lat = geoLocation.get()[0];
                    lng = geoLocation.get()[1];
                } else {
                    logger.warn("Failed to fetch geolocation for city: {}, country: {}", city, country);
                }
            }

            // Create a new row with updated latitude and longitude values
            Object[] values = new Object[row.size()];
            for (int i = 0; i < row.size(); i++) {
                values[i] = row.get(i);
            }
            values[row.fieldIndex("lat")] = lat;
            values[row.fieldIndex("lng")] = lng;
            return RowFactory.create(values); // Return the modified row
        }, RowEncoder.apply(schema)); // Return the dataset with the modified rows
    }

    // Method to fetch geolocation from the OpenCage API
    private Optional<double[]> fetchGeoLocationFromApi(String city, String country) {
        try {
            // Create the query string and URL for the API call
            String query = String.format("%s, %s", city, country);
            String urlString = String.format("%s?q=%s&key=%s", OPEN_CAGE_API_URL, query.replace(" ", "%20"), OPEN_CAGE_API_KEY);
            URL url = new URL(urlString);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            // Read the response from the API
            InputStreamReader reader = new InputStreamReader(connection.getInputStream());
            StringBuilder responseBuilder = new StringBuilder();
            int c;
            while ((c = reader.read()) != -1) {
                responseBuilder.append((char) c);
            }

            // Parse the JSON response and extract latitude and longitude
            JSONObject responseJson = new JSONObject(responseBuilder.toString());
            if (responseJson.has("results") && !responseJson.getJSONArray("results").isEmpty()) {
                JSONObject geometry = responseJson.getJSONArray("results").getJSONObject(0).getJSONObject("geometry");
                return Optional.of(new double[]{geometry.getDouble("lat"), geometry.getDouble("lng")});
            }
        } catch (Exception e) {
            logger.error("Error fetching geolocation: {}", e.getMessage());
        }
        return Optional.empty(); // Return empty if geolocation could not be fetched
    }
}
