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

  Logger.getLogger("org").setLevel(Level.ERROR)
  val conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]")
  val sc = new SparkContext(conf)

  // read the word count text file
  val lines = sc.textFile("in/word_count.text")
  // get a list of all words seperated to a regular RDD using flatMap()
  val wordRdd = lines.flatMap(line => line.split(" "))

  // create paird RDD by assigning value 1 in-front to each key (word)
  val wordPairRdd = wordRdd.map(word => (word, 1))
  // run reduceByKey() transaction to get the count (no of occurances of each word(keys))
  val wordToCountPairs = wordPairRdd.reduceByKey((x, y) => x + y)

  // to sort, first flip the kry and value (as sort can be done only on keys)
  val countToWordParis = wordToCountPairs.map(wordToCount => (wordToCount._2, wordToCount._1))

  // call sortByKey() to sort. : ascending = false, will sort RDD elements in descending order
  val sortedCountToWordParis = countToWordParis.sortByKey(ascending = false)

  // finally flip the key:value pair of PairRDD again to transform it to its initial state.
  val sortedWordToCountPairs = sortedCountToWordParis.map(countToWord => (countToWord._2, countToWord._1))

  for ((word, count) <- sortedWordToCountPairs.collect()) println(word + " : " + count)
}

