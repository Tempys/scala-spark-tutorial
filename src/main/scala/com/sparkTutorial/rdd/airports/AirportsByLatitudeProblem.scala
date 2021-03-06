package com.sparkTutorial.rdd.airports

import com.sparkTutorial.commons.Utils
import org.apache.spark.{SparkConf, SparkContext}

object AirportsByLatitudeProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
       Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

       Each row of the input file contains the following columns:
       Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
       ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

       Sample output:
       "St Anthony", 51.391944
       "Tofino", 49.082222
       ...
     */

    var conf = new SparkConf().setAppName("AirportsByLatitude").setMaster("local[2]")
    var context = new SparkContext(conf)

      val  airports = context.textFile("in/airports.text")

      var result =   airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toDouble > 40 )

    val airportsNameAndCityNames = result.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })

    airportsNameAndCityNames.foreach(line => print(line))

    airportsNameAndCityNames.saveAsTextFile("out/airports_in_usa.text")




  }
}
