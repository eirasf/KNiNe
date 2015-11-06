package es.udc.graph.test.utils

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

/**
 * Created by David Martinez Rego on 11/06/15.
 */
object SparkTestUtil {

  var sc : SparkContext= null

  def context(): SparkContext = {
    this.synchronized {
      if (sc == null) {
        val sparkConf = new SparkConf().setAppName("test minTweets").set("spark.driver.allowMultipleContexts", "true")
        sparkConf.setMaster("local")
        sc = new SparkContext(sparkConf)
      }
      sc
    }
  }
}