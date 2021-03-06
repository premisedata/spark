package spark.streaming.util

import spark.SparkContext
import spark.SparkContext._
import it.unimi.dsi.fastutil.objects.{Object2LongOpenHashMap => OLMap}
import scala.collection.JavaConversions.mapAsScalaMap

object RawTextHelper {

  /** 
   * Splits lines and counts the words in them using specialized object-to-long hashmap 
   * (to avoid boxing-unboxing overhead of Long in java/scala HashMap)
   */
  def splitAndCountPartitions(iter: Iterator[String]): Iterator[(String, Long)] = {
    val map = new OLMap[String]
    var i = 0
    var j = 0
    while (iter.hasNext) {
      val s = iter.next()
      i = 0
      while (i < s.length) {
        j = i
        while (j < s.length && s.charAt(j) != ' ') {
          j += 1
        }
        if (j > i) {
          val w = s.substring(i, j)
          val c = map.getLong(w)
          map.put(w, c + 1)
        }
        i = j
        while (i < s.length && s.charAt(i) == ' ') {
          i += 1
        }
      }
    }
    map.toIterator.map{case (k, v) => (k, v)}
  }

  /** 
   * Gets the top k words in terms of word counts. Assumes that each word exists only once
   * in the `data` iterator (that is, the counts have been reduced).
   */
  def topK(data: Iterator[(String, Long)], k: Int): Iterator[(String, Long)] = {
    val taken = new Array[(String, Long)](k)
    
    var i = 0
    var len = 0
    var done = false
    var value: (String, Long) = null
    var swap: (String, Long) = null
    var count = 0

    while(data.hasNext) {
      value = data.next
      if (value != null) {
        count += 1
        if (len == 0) {
          taken(0) = value
          len = 1
        } else if (len < k || value._2 > taken(len - 1)._2) {
          if (len < k) {
            len += 1
          }
          taken(len - 1) = value
          i = len - 1
          while(i > 0 && taken(i - 1)._2 < taken(i)._2) {
            swap = taken(i)
            taken(i) = taken(i-1)
            taken(i - 1) = swap
            i -= 1
          }
        }
      }
    }
    return taken.toIterator  
  }
 
  /**
   * Warms up the SparkContext in master and slave by running tasks to force JIT kick in
   * before real workload starts.
   */
  def warmUp(sc: SparkContext) {
    for(i <- 0 to 1) {
      sc.parallelize(1 to 200000, 1000)
        .map(_ % 1331).map(_.toString)
        .mapPartitions(splitAndCountPartitions).reduceByKey(_ + _, 10)
        .count()
    }
  }
  
  def add(v1: Long, v2: Long) = (v1 + v2) 

  def subtract(v1: Long, v2: Long) = (v1 - v2) 

  def max(v1: Long, v2: Long) = math.max(v1, v2) 
}

