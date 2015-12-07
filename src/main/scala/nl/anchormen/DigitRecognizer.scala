package nl.anchormen

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.PCA

case class DigitCluster (digit: Int, cluster: Int, count: Int) //used for converting RDD to Dataframe using reflection!

/**
 * @author Mohamed El Sioufy <m.sioufy@anchormen.nl>
 */
object DigitRecognizer {
  def main(args: Array[String]) {
    assert(args.size > 0, "Please supply the (HDFS) path to the modified MNIST digits dataset")
    val path = args(0)
    setLogsToErrorOnly //disabling spark logging warning messages

    // Set up
    val sparkConf = new SparkConf()
      .setAppName("Digits Clusters")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext) /*required in this example to load the data from a .csv file*/

    val labeledTrainingData = loadTrainingData(sqlContext, path) /*loading the digits labeled data (label, featuresVector)*/
    
    /******************************************************************************************************************/
    /*each image has 28x28 = 784  pixels (presenting its features)
    * we do dimensionality reduction using PCA and use the first 64 principal components as our new features*/
    val pca = new PCA(64).fit(labeledTrainingData.map(_._2))
    val reducedLabeledTrainingData = labeledTrainingData.map(lp => (lp._1, pca.transform(lp._2)))
    
    /******************************************************************************************************************/
    /*parameters for the k-means algorithm*/
    val k = 10 //we have 10 numbers 0-9 <- we aim to cluster each number to its own cluster (might not be the end-case though)
    val maxIterations = 200 //max # of iterations to be considered by the algorithm  -- can reduce this to ex.100 to get better performance (vs accuracy)
    
    /*the number of times to run the k-means algorithm
    k-means is not guaranteed to find a globally optimal solution
    when run multiple timfes on a given dataset, the algorithm returns the best clustering result
    We won't do the initialization using the Kmeans|| method so better run the algorithms multiple times
    */
    val runs = 10
    
    /******************************************************************************************************************/
    /*running the algorithm*/
    val digitClusters = KMeans.train(reducedLabeledTrainingData.map(_._2).cache(), k, maxIterations)

    /*clustering & info*/
    import sqlContext.implicits._
    val clusterInfoDataframe = reducedLabeledTrainingData
      .mapValues(features => digitClusters.predict(features))   // (label, cluster)
      .map(x => (x,1))    //((label,cluster), 1)
      .reduceByKey(_+_)   //((label,cluster), # of times this label has been predicted to this cluster)
      .map{ case ((digit, cluster), count) => DigitCluster(digit, cluster, count)}
    
      /*this digit was mapped to this cluster count times
      * best case scenario, each digit shall be always mapped to a unique individual cluster
      * however, since some digits have similar shapes (ex. 3,8) different digits could be mapped to similar clusters
      * that is the case in fact*/
      .toDF()

    clusterInfoDataframe.show()
    /*similarities between 7,4 and 9
    * similarities between 2 & 6 !?
    * similarities between 3 and 8*/
  }
  
  def loadTrainingData(sqlContext : SQLContext, path : String) : RDD[(Int,Vector)] = {
    import com.databricks.spark.csv._
    val dataMatrix = sqlContext.csvFile(path, false)

    dataMatrix.rdd
      .map( row => (row.toSeq.head.toString.toInt, row.toSeq.tail.map(_.toString.toDouble))) // (label, features as sequence)
      .mapValues( featuresSeq => Vectors.dense(featuresSeq.head, featuresSeq.tail: _*)) // (label, features as vector)
    /*no need to use a LabeledPoint since we're not making use of it in the rest of the code!*/
  }

  def setLogsToErrorOnly = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("streaming").setLevel(Level.ERROR)
    Logger.getLogger("spark").setLevel(Level.ERROR)
  }


}