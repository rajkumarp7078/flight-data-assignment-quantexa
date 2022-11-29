package com.quantexa.flightanalytics.service

import com.quantexa.flightanalytics.config.AppConfig
import com.quantexa.flightanalytics.models.DataModels.{Flight, FlightPerMonth, MostFrequentFlyers, NFlightsTogether, Passenger, PassengersFlyingTghtr, PassengersLongestRun}
import com.quantexa.flightanalytics.utils.{SparkSessionWrapper, Utils}
import org.apache.spark.sql.{Dataset, SaveMode}
import org.apache.spark.sql.functions._

import java.sql.Date

class FlightAnalyticsService extends SparkSessionWrapper with AppConfig {

  import spark.implicits._
  import java.nio.file.Paths
  val outputPath = Paths.get("").toAbsolutePath+"/output"

  /**
   * creates flights dataset and cache
   *
   * @return
   */
  def getFlightDataset(): Dataset[Flight] = {
    val flightFileName = conf.getString("flight-analytics.config.flight-file-name")
    val flightDs: Dataset[Flight] = Utils.createDataframe(flightFileName).as[Flight]
    flightDs.cache()
    flightDs
  }

  /**
   * creates Passengers dataset and cache
   *
   * @return
   */
  def getPassengersDataset(): Dataset[Passenger] = {
    val passengersFileName = conf.getString("flight-analytics.config.passengers-file-name")
    val passengersDs: Dataset[Passenger] = Utils.createDataframe(passengersFileName).as[Passenger]
    passengersDs.cache()
    passengersDs
  }

  /**
   * Find the total number of flights for each month.
   *
   * @return flightsEachMonthDs
   */
  def getTotalNumberFlightsEachMonth(): Dataset[FlightPerMonth] = {
    val flightDs: Dataset[Flight] = getFlightDataset()

    val flightsDsTransformed = flightDs.select(col("flightId"), col("date"), col("from"), col("to"),
      date_format(to_date(col("date"), "yyyy-MM-dd"), "MM").as("Month").cast("int")).dropDuplicates()

    val flightsEachMonthDs = flightsDsTransformed.groupBy("Month")
      .agg(sum("flightId").as("numberOfFlights"))
      .orderBy("month")
      .as[FlightPerMonth]

    flightsEachMonthDs.show(false)
    flightsEachMonthDs.coalesce(1).write.mode(SaveMode.Overwrite).option("header",true).csv(outputPath+"/flightsEachMonth")
    flightsEachMonthDs
  }

  /**
   * Find the names of the 100 most frequent flyers.
   *
   * @return mostFreqFlyersDs
   */
  def getMostFrequentFlyers(): Dataset[MostFrequentFlyers] = {
    val flightDs: Dataset[Flight] = getFlightDataset()
    val flightAggDs = flightDs.groupBy("passengerId").agg(count("flightId").as("numberOfFlights"))

    val passengersDs: Dataset[Passenger] = getPassengersDataset()
    val mostFreqFlyersDs = flightAggDs.as("flights").join(passengersDs.as("passengers"),
      flightAggDs("passengerId") === passengersDs("passengerId")).select("flights.passengerId",
      "flights.numberOfFlights", "passengers.firstName", "passengers.lastName")
      .orderBy(col("numberOfFlights").desc).as[MostFrequentFlyers]

    mostFreqFlyersDs.show( false)
    mostFreqFlyersDs.coalesce(1).write.mode(SaveMode.Overwrite).option("header",true).csv(outputPath+"/mostFreqFlyers")
    mostFreqFlyersDs
  }


  /***
   * Find the greatest number of countries a passenger has been in without being in the UK
   *
   * @return passengerLongestRunsDs
   */
  def getCountriesPassengersUk(): Dataset[PassengersLongestRun] = {

    val flightDs: Dataset[Flight] = getFlightDataset()

    // first we need to group by passenger to collect all his "from" countries
    val dataWithCountries = flightDs.groupBy("passengerId").agg(
      concat(collect_list(col("from"))).name("countries")
    )

    //udf to get max number of countries travelled
    val longestRun = (countries: Array[String]) => {
      if (countries.contains("uk")) {
        val countriesList = countries.mkString(" ")
          .split("uk")
          //condition for only one trip to uk
          .drop(1).dropRight(1)
          .filter(_.nonEmpty)
          //condition for trip from uk to uk
          .filter(_ != " ")
          .map(_.trim)
        if (countriesList.length > 0) {
          countriesList.map(s => s.split(" ").length)
            .max
        } else 0
      } else 0
    }

    val longestRunUdf = udf(longestRun)

    val passengerLongestRunsDs: Dataset[PassengersLongestRun] = dataWithCountries.withColumn("LongestRun",
      longestRunUdf(col("countries"))).filter("LongestRun !=0").orderBy(col("LongestRun").desc)
      .select("passengerId", "longestRun").as[PassengersLongestRun]
    passengerLongestRunsDs.show( false)
    passengerLongestRunsDs.coalesce(1).write.mode(SaveMode.Overwrite).option("header",true).csv(outputPath+"/passengerLongestRuns")
    passengerLongestRunsDs
  }

  /**
   * Find the passengers who have been on more than 3 flights together.
   *
   * @return passengersFlyingTghtrDs
   */
  def getPassengersFlyingTghtr(): Dataset[PassengersFlyingTghtr] = {

    val flightDs: Dataset[Flight] = getFlightDataset()

    val passengersFlyingTghtrDs = flightDs.as("df1").join(flightDs.as("df2"),
        col("df1.flightId") === col("df2.flightId") &&
        col("df1.date") === col("df2.date"),
      "inner"
    ).groupBy(col("df1.passengerId"), col("df2.passengerId"))
      .agg(count("*").as("numberOfFlightsTogether"))
      .where(col("numberOfFlightsTogether") >= 3)
      .select(col("df1.passengerId").as("passenger1Id"), col("df2.passengerId").as("passenger2Id"),
        col("numberOfFlightsTogether")).orderBy(col("numberOfFlightsTogether").desc).as[PassengersFlyingTghtr]

    passengersFlyingTghtrDs.show(false)
    passengersFlyingTghtrDs.coalesce(1).write.mode(SaveMode.Overwrite).option("header",true).csv(outputPath+"/passengersFlyingTghtr")
    passengersFlyingTghtrDs
  }

  /**
   * Find the passengers who have been on more than N flights together within the range (from,to).
   *
   * @param atLeastNTimes
   * @param from
   * @param to
   * @return NPassengersFlownTogetherDs
   */
  def flownTogether(atLeastNTimes: Int, from: Date, to: Date): Dataset[NFlightsTogether] = {

    val flightDs: Dataset[Flight] = getFlightDataset()

    val NPassengersFlownTogetherDs = flightDs.where(col("date") >= lit(from) && col("date") <= lit(to))
      .as("df1").join(flightDs.as("df2"),
        col("df1.flightId") === col("df2.flightId") &&
        col("df1.date") === col("df2.date"),
      "inner"
    ).groupBy(col("df1.passengerId"), col("df2.passengerId"))
      .agg(count("*").as("numberOfFlightsTogether"),min(col("df1.date")).as("from"), max(col("df1.date")).as("to"))
      .where(col("numberOfFlightsTogether") >= atLeastNTimes)
      .select(col("df1.passengerId").as("passenger1Id"), col("df2.passengerId").as("passenger2Id"),
        col("numberOfFlightsTogether"),col("from"),col("to"))
      .select("passenger1Id","passenger2Id","numberOfFlightsTogether","from","to")
      .orderBy(col("numberOfFlightsTogether").desc)
      .as[NFlightsTogether]

    NPassengersFlownTogetherDs.show(false)
    NPassengersFlownTogetherDs.coalesce(1).write.mode(SaveMode.Overwrite).option("header",true).csv(outputPath+"/NPassengersFlownTogether")
    NPassengersFlownTogetherDs.as[NFlightsTogether]
  }

}
