package com.sparkTutorial.pairRdd.aggregation.reducebykey

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    // read the text file
    val lines = sc.textFile("in/word_count.text")
    // create a regular RDD of words in the text file using flatMap
    val wordRdd = lines.flatMap(line => line.split(" "))

    // creating a pair RDD with each word as the key, and 1 as the value
    val wordPairRdd = wordRdd.map(word => (word, 1))

    // apply reduceByKey method to get summation of each unique key field (no Of occurences)
    val wordCounts = wordPairRdd.reduceByKey((x, y) => x + y)
    for ((word, count) <- wordCounts.collect()) println(word + " : " + count)
  }
}
