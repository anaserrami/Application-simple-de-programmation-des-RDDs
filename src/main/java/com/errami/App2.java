package com.errami;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class App2 {
    public static void main(String[] args) {
        // Initialize Spark context
        SparkConf conf = new SparkConf().setAppName("App2").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Year to filter by (example: 2024)
        String targetYear = "2024";

        // Load the data
        JavaRDD<String> data = sc.textFile("./ventes.txt");

        // Split each line, filter by year, and map city with price
        JavaPairRDD<String, Double> salesByCityForYear = data
                .filter(line -> line.startsWith(targetYear)) // Filter by the year
                .mapToPair(line -> {
                    String[] fields = line.split(" ");
                    String city = fields[1];
                    double price = Double.parseDouble(fields[3]);
                    return new Tuple2<>(city, price);
                })
                .reduceByKey(Double::sum);  // Aggregate the sales by city

        // Collect and print the result
        salesByCityForYear.collect().forEach(tuple ->
                System.out.println("City: " + tuple._1() + ", Total Sales: " + tuple._2())
        );

        // Stop the Spark context
        sc.stop();
    }
}