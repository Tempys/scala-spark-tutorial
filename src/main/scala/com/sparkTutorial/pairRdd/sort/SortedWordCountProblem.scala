package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("averageHousePriceSolution").setMaster("local[3]")
    val sc = new SparkContext(conf)


    val lines = sc.textFile("in/word_count.text")
    var result = lines.flatMap(line => line.split(" "))
                      .map(word => (word,1))
                      .reduceByKey(_+_)
                      .sortBy(_._2,false)

    for((k,v) <- result.collect()) println((k+" "+v))



  }

}

