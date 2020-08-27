package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsInUsaProblem {
  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
       and output the airport's name and the city's name to out/airports_in_usa.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "Putnam County Airport", "Greencastle"
       "Dowagiac Municipal Airport", "Dowagiac"
       ...
     */

      val conf = new SparkConf().setAppName("airports").setMaster("local[2]") // allocation 2 local CPUs
      val sc = new SparkContext(conf) // new SparkContext instance (Entry Point)

      val airports = sc.textFile("in/airports.text") // load RDD
      val airportsInUSA = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(3) == "\"United States\"")
      // COMMA_DELIMITER - find values seperated by commas - using Regular Expressions ( Not inside quotes)
      // (3) means, 4th Index (starts from 0)
      // All countries has "" so we include it into query as escape characters

      val airportsNameAndCityNames = airportsInUSA.map(line => {
        val splits = line.split(Utils.COMMA_DELIMITER)
        splits(1) + ", " + splits(2) // splitting result into Airport name and City name with Delimeter
      })
      airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text") // save output to a text file
    }
}
