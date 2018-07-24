package com.sparkTutorial.sparkStream

import org.apache.log4j.{Level, Logger}

import org.apache.spark.{SparkConf, SparkContext}

class SimpleStreamKafka {


  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

  }

}
