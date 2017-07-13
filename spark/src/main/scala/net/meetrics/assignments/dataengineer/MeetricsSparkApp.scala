package net.meetrics.assignments.dataengineer

import org.apache.spark.{SparkConf, SparkContext}

trait MeetricsSparkApp extends App {

    val sparkConf = new SparkConf()
        .setMaster("local[1]")
        .setAppName("dataengineer-assignment")

    val sparkContext = new SparkContext(sparkConf)

}