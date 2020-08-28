package com.sparkTutorial.pairRdd.create

import org.apache.spark.{SparkConf, SparkContext}

object PairRddFromRegularRdd {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("create").setMaster("local")
    val sc = new SparkContext(conf)

    // create a string list
    val inputStrings = List("Lily 23", "Jack 29", "Mary 29", "James 8")
    // creating a regular RDD by calling parallelize() method
    val regularRDDs = sc.parallelize(inputStrings)

    // Creating a pair RDD with mapping first tow values in each line , as the key:value pair of pairRDD
    val pairRDD = regularRDDs.map(s => (s.split(" ")(0), s.split(" ")(1)))
    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_regular_rdd")
  }
}
