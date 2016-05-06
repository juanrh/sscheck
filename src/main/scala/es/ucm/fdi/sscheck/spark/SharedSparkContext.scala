package es.ucm.fdi.sscheck.spark

import org.apache.spark._

import org.slf4j.LoggerFactory

/** This trait can be used to share a Spark Context. The context is created
 *  the first time sc() is called, and stopped when close() is called
 * */
trait SharedSparkContext
  extends Serializable
  with java.io.Closeable { 
  
  // cannot use private[this] due to https://issues.scala-lang.org/browse/SI-8087
  //@transient private[this] val logger = Logger(LoggerFactory.getLogger("SharedSparkContext"))
  @transient private val logger = LoggerFactory.getLogger("SharedSparkContext")
  
  /** Override for custom config
  * */
  def sparkMaster : String 
    
  /** Override for custom config
  * */
  def sparkAppName : String = "ScalaCheck Spark test"
  
  // lazy val so early definitions are not needed for subtyping
  @transient lazy val conf = new SparkConf().setMaster(sparkMaster).setAppName(sparkAppName)    
  
  @transient protected[this] var _sc : Option[SparkContext] = None
  def sc() : SparkContext = { 
    _sc.getOrElse {
      logger.warn("creating test Spark context")
      _sc = Some(new SparkContext(conf))
      _sc.get
    }
  }
  
  override def close() : Unit = {
    _sc.foreach { sc => 
      logger.warn("stopping test Spark context")
      sc.stop()
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
    }
    _sc = None
  }
}