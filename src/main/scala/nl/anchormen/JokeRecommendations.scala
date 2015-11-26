package nl.anchormen

import org.apache.spark.SparkContext

/**
 * @author Jeroen Vlek <j.vlek@anchormen.nl>
 */
object JokeRecommendations {
  
  def main(args : Array[String]) {
    assert(args.size > 1, "Please supply the (hdfs) path to the Jester dataset")
  }
  
}