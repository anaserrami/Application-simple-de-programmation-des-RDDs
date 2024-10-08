package com.errami;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class App1 {
    public static void main(String[] args) {
        // Initialize Spark context
        SparkConf conf = new SparkConf().setAppName("App1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load the data
        JavaRDD<String> data = sc.textFile("./ventes.txt");

        // Split each line and map city with price
        JavaPairRDD<String, Double> salesByCity = data
                .mapToPair(line -> {
                    String[] fields = line.split(" ");
                    String city = fields[1];
                    double price = Double.parseDouble(fields[3]);
                    return new Tuple2<>(city, price);
                })
                .reduceByKey(Double::sum);  // Aggregate the sales by city

        // Collect and print the result
        salesByCity.collect().forEach(tuple ->
                System.out.println("City: " + tuple._1() + ", Total Sales: " + tuple._2())
        );

        // Stop the Spark context
        sc.stop();
    }
}