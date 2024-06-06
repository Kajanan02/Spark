package com.sparkSession.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SumOfPrimes {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        SparkConf conf = new SparkConf().setAppName("Actions").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/primes.text");

        List<Integer> primesList = lines.flatMap(line -> Arrays.asList(line.trim().split("\\s+")).iterator())
                .map(Integer::parseInt)
                .take(100);

        JavaRDD<Integer> primesRDD = sc.parallelize(primesList);

        int sum = primesRDD.reduce(Integer::sum);
        System.out.println("Sum of the first 100 prime numbers: " + sum);

    }
}