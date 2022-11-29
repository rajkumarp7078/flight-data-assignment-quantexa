package com.quantexa.flightanalytics.models

import java.sql.Date


object DataModels {

  /******************** Input Case Classes ********************************************/
  case class Flight(passengerId:Int, flightId:Int, from:String, to:String, date:String)
  case class Passenger(passengerId:Int, firstName:String, lastName:String)

  /******************** Output Case Classes ********************************************/
  case class FlightPerMonth(Month:Int, numberOfFlights:Long)
  case class MostFrequentFlyers(passengerId:Int, numberOfFlights:Long, firstName:String, lastName:String)
  case class PassengersLongestRun(passengerId:Int, LongestRun:Int)
  case class PassengersFlyingTghtr(passenger1Id:Int, passenger2Id:Int, numberOfFlightsTogether:Long)
  case class NFlightsTogether(passenger1Id:Int, passenger2Id:Int, numberOfFlightsTogether:Long, from:String, to:String)

}
