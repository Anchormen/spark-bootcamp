package nl.anchormen

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * @author Jeroen Vlek <j.vlek@anchormen.nl>
 */
object JokeRecommendations {
  
  def main(args : Array[String]) {
    assert(args.size > 0, "Please supply the (hdfs) path to the Jester dataset")
    
    val sparkConf = new SparkConf()
      .setAppName("Joke Recommendations")
  
   val sparkContext = new SparkContext(sparkConf)
    
   sparkContext.stop()
  }
  
}