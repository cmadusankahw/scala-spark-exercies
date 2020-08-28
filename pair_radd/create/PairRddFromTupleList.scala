package com.sparkTutorial.pairRdd.create

import org.apache.spark.{SparkConf, SparkContext}

object PairRddFromTupleList {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("create").setMaster("local[1]")
    val sc = new SparkContext(conf)

    // creating a tuple
    val tuple = List(("Lily", 23), ("Jack", 29), ("Mary", 29), ("James", 8))

    // directly create a pairRDD calling parallelize() method on tuple
    val pairRDD = sc.parallelize(tuple)

    // according to docs, this method will stop shuffling, and process the action in a single node
    pairRDD.coalesce(1).saveAsTextFile("out/pair_rdd_from_tuple_list")
  }
}
