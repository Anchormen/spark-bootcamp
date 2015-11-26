package nl.anchormen

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD

/**
 * @author Jeroen Vlek <j.vlek@anchormen.nl>
 */
object JokeRecommendations {
  
  def main(args : Array[String]) {
    assert(args.size > 0, "Please supply the (hdfs) path to the Jester dataset")
    val path = args(0)
    
    val sparkConf = new SparkConf()
      .setAppName("Joke Recommendations")
  
   val sparkContext = new SparkContext(sparkConf)
   val sqlContext = new SQLContext(sparkContext)
   
   val ratings = loadRatings(sqlContext, path)
   
   sparkContext.stop()
  }
  
  def loadRatings(sqlContext : SQLContext, path : String) : DataFrame = {
	  import com.databricks.spark.csv._
    val ratingsMatrix = sqlContext.csvFile(path)
    
    // we need to flatten the Dataframe from {user_id, rating_item1, rating_item2, rating_item3...} to {user, item, rating} 
    val flatRatingsRdd = ratingsMatrix.flatMap( { row =>
        (1 to row.size - 1).map(itemIndex => (row.getString(0), itemIndex, row.getDouble(itemIndex)))
    })
      
    import sqlContext.implicits._
    flatRatingsRdd.toDF("user", "item", "rating")
  }
  
}