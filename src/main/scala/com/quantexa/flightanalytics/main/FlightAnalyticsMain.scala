package com.quantexa.flightanalytics.main

import com.quantexa.flightanalytics.service.FlightAnalyticsService
import com.quantexa.flightanalytics.utils.SparkSessionWrapper

import java.sql.Date
import java.text.SimpleDateFormat


object FlightAnalyticsMain extends App with SparkSessionWrapper {

  val flightAnalyticsService = new FlightAnalyticsService()
  flightAnalyticsService.getTotalNumberFlightsEachMonth()
  flightAnalyticsService.getCountriesPassengersUk()
  flightAnalyticsService.getPassengersFlyingTghtr()

  val fromDateStr = "2017-01-01"
  val toDateStr = "2017-01-30"
  val formatter = new SimpleDateFormat("yyyy-MM-dd")

  val fromDateStrFormatted = formatter.parse(fromDateStr)
  val from:Option[Date]  = Option(new java.sql.Date(fromDateStrFormatted.getTime()))
  println(from)

  val toDateStrFormatted = formatter.parse(toDateStr)
  val to:Option[Date]  = Option(new java.sql.Date(toDateStrFormatted.getTime()))
  println(to)
  flightAnalyticsService.flownTogether(3,from.get,to.get)

}
