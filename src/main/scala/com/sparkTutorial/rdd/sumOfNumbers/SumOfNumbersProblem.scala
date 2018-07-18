package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */

    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("primeNumbers").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/prime_nums.text")

    var sum = lines.take(100)
                   .flatMap(item => item.split("(\t| )*"))
                   .filter(item => item != "")
                   .map(item => item.toInt)
                   .sum

    print("the sum is: "+sum)

  }
}
