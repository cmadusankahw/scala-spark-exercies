package com.sparkTutorial.rdd

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark._

object WordCount {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val words = lines.flatMap(line => line.split(" ")) // split words into a list and flteen all individual lists to one single list with flatMap()

    val wordCounts = words.countByValue() // count words using countByValue() which returns the count of all distinct value together as a histogram
    for ((word, count) <- wordCounts) println(word + " : " + count) // print word counts with a for loop
  }
}
