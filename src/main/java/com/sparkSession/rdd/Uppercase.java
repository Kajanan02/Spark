package com.sparkSession.rdd;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Uppercase {

    public static void main(String[] args) throws Exception {
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("uppercase").setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> cities = sc.textFile("in/cities.text");
        JavaRDD<String> cities1 = cities.filter(line -> line.split(",")[3].equals("\"United States\""));
        JavaPairRDD<String, String> nameAndCountryNameRdd = cities1.mapToPair(getNameAndCountryNamePair());
        JavaPairRDD<String, String> uppercase = nameAndCountryNameRdd.mapValues(country -> country.toUpperCase());


//        JavaRDD<String> lowerCaseLines = lines.map(line -> line.toUpperCase());

//        lowerCaseLines.saveAsTextFile("out/uppercase.text");
//        JavaRDD<String, String> nameAndCountryNameRdd = lines.mapToPair(getNameAndCountryNamePair());
//        nameAndCountryNameRdd.saveAsTextFile("out/nameAndCountryName.text");
//        JavaRDD<String, String> uppercase = nameAndCountryNameRdd.mapValues(country -> country.toUpperCase());
        uppercase.coalesce(1).saveAsTextFile("out/uppercase.text");
    }

    private static PairFunction<String, String, String> getNameAndCountryNamePair() {
        return (PairFunction<String, String, String>) line -> new Tuple2<>(line.split(",")[1], line.split(",")[3]);
    }
}
