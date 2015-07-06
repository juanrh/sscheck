package es.ucm.fdi.sscheck.testing

import org.apache.spark._

/** This trait can be used to share a Spark Context. The context is created
 *  the first time sc() is called, and stopped when close() is called
 * */
trait SharedSparkContext
  extends Logging 
  with Serializable
  with java.io.Closeable {
  // TODO: replace prints with logging?
  
  /** Override for custom config
  * */
  def master : String = "local[4]"
  def appName : String = "scalacheck Spark test"
  
  // lazy val so early definitions are not needed for subtyping
  @transient lazy val conf = new SparkConf().setMaster(master).setAppName(appName)    
  
  @transient protected[this] var _sc : Option[SparkContext] = None
  def sc() : SparkContext = { 
    _sc.getOrElse({
      println("creating test Spark context")
      _sc = Some(new SparkContext(conf))
      _sc.get
    })
  }
  
  def close() : Unit = {
    _sc.foreach{sc => 
      println("stopping test Spark context")
      sc.stop()
      // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
      System.clearProperty("spark.driver.port")
    }
    _sc = None
  }
}