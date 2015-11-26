package nl.anchormen

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.recommendation.ALS
import scala.util.Try

/**
 * @author Jeroen Vlek <j.vlek@anchormen.nl>
 */
object JokeRecommendations {
  
  def main(args : Array[String]) {
    assert(args.size > 0, "Please supply the (hdfs) path to the Jester dataset")
    val path = args(0)

   // Set up
   val sparkConf = new SparkConf()
      .setAppName("Joke Recommendations")
   val sparkContext = new SparkContext(sparkConf)
   val sqlContext = new SQLContext(sparkContext)
   
   // Get data in right format
   val ratings = loadRatings(sqlContext, path)
   
   // Train model
   val alsEstimator = new ALS
   val model = alsEstimator.fit(ratings)
   
   sparkContext.stop()
  }
  
  def loadRatings(sqlContext : SQLContext, path : String) : DataFrame = {
	  import com.databricks.spark.csv._
    val ratingsMatrix = sqlContext.csvFile(path, false)
    
    // just generate user ids, this can also be used for generating primary keys
    val withUserIds = ratingsMatrix.rdd.zipWithUniqueId
    
    // we need to reduce the dimensionality of the matrix 
    // from {numJokes, rating_item1, rating_item2, rating_item3...} 
    // to {user, item, rating} 
    val flatRatingsRdd = withUserIds.flatMap( {
      case (row : Row, userId : Long) =>
        (1 to row.size - 1).map(itemIndex => Try(userId, itemIndex, row.getString(itemIndex).toDouble).toOption).flatten
    })
      
    import sqlContext.implicits._
    flatRatingsRdd.toDF("user", "item", "rating").where($"rating" < 99)
  }
  
}