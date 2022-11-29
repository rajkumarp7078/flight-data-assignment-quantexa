package com.quantexa.flightanalytics.utils

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master("local[*]")
      .appName("QuantexaFlightAnalytics")
      .getOrCreate()
  }
  spark.sparkContext.setLogLevel("ERROR")
}
