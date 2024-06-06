package com.sparkSession.rdd.commons;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;

public class RealEstate {
    public static void main(String[] args) throws Exception {
        SparkSession session = SparkSession.builder().appName("SparkSQL").master("local[2]").getOrCreate();
        DataFrameReader dataFrameReader = session.read();

        Dataset<Row> responses = dataFrameReader.option("header", "true").option("inferSchema", "true").csv("in/RealEstate.csv");

//        System.out.println("=== Print some records by using groups ===");
//        RelationalGroupedDataset groupedDataset = responses.groupBy(col("Location"));
//        groupedDataset.count().show();
        Dataset<Row> aggregatedDF = responses.groupBy("Location")
                .agg(avg("Price SQ Ft").alias("avg(Price SQ Ft)"), max("Price").alias("max(Price)"));

      Dataset<Row> sortedDF = aggregatedDF.orderBy(functions.col("avg(Price SQ Ft)"));
        sortedDF.show();


    }
}
