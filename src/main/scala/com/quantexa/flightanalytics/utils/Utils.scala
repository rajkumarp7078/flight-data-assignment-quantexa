package com.quantexa.flightanalytics.utils

import com.quantexa.flightanalytics.config.AppConfig
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}
import org.apache.spark.sql.functions.{col, to_date}


object Utils extends SparkSessionWrapper with AppConfig {

  private val inputPath = conf.getString("flight-analytics.config.input-path")

  /**
   * creates the dataframe from input file
   *
   * @param fileName
   * @return
   */
  def createDataframe(fileName: String): DataFrame ={
    val df = spark.read.options(Map("inferSchema" -> "true", "header" -> "true")).csv(inputPath+fileName)
    df
  }

}
