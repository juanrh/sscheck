package es.ucm.fdi.sscheck.spark

import org.apache.spark._
import com.typesafe.scalalogging.slf4j.Logging

/** This trait can be used to share a Spark Context. The context is created
 *  the first time sc() is called, and stopped when close() is called
 * */
trait SharedSparkContext
  extends Serializable
  with java.io.Closeable 
  with com.typesafe.scalalogging.slf4j.Logging {
  
  /** Override for custom config
  * */
  def sparkMaster : String = "local[4]"
    
  /** Override for custom config
  * */
  def sparkAppName : String = "scalacheck Spark test"
  
  // lazy val so early definitions are not needed for subtyping
  @transient lazy val conf = new SparkConf().setMaster(sparkMaster).setAppName(sparkAppName)    
  
  @transient protected[this] var _sc : Option[SparkContext] = None
  def sc() : SparkContext = { 
    _sc.getOrElse({
      logger.info("creating test Spark context")
      _sc = Some(new SparkContext(conf))
      _sc.get
    })
  }
  
  def close() : Unit = {
    _sc.foreach{sc => 
      logger.info("stopping test Spark context")
      sc.stop()
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
    }
    _sc = None
  }
}