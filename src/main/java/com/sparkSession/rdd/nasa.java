package com.sparkSession.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class nasa {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("Nasa").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> firstLogs = sc.textFile("in/nasa_log_01.tsv");
        JavaRDD<String> secondLogs = sc.textFile("in/nasa_log_02.tsv");

        JavaPairRDD<String, Integer> hosts1 = firstLogs.mapToPair(line -> new Tuple2<>(line.split("\t")[0], 1));
        JavaPairRDD<String, Integer> hosts2 = secondLogs.mapToPair(line -> new Tuple2<>(line.split("\t")[0], 1));

        JavaPairRDD<String, Integer> commonHosts = hosts1.join(hosts2).mapToPair(pair -> new Tuple2<>(pair._1(), 1)).reduceByKey(Integer::sum);

        commonHosts.map(pair -> pair._1()).saveAsTextFile("out/same_hosts_nasa_logs.csv");

    }

    private static boolean isNotHeader(String line){
        return !(line.startsWith("host") && line.contains("bytes"));
    }
}
