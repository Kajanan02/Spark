package com.sparkSession.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class airportCities {
    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("airport").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> airportData = sc.textFile("in/cities.text");
        JavaRDD<String> airportDataFilter = airportData.filter(line -> Double.parseDouble(line.split(",")[6]) > 40.0);
        JavaPairRDD<String, String> nameAndCountryNameRdd = airportDataFilter.mapToPair(getNameAndCountryNamePair());

        nameAndCountryNameRdd.coalesce(1).saveAsTextFile("out/cities_by_latitude.text");
    }

    private static PairFunction<String, String, String> getNameAndCountryNamePair() {
        return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split(",")[1], line.split(",")[6]);
    }
}
