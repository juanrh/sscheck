package es.ucm.fdi.sscheck.spark.streaming

import org.apache.spark.streaming.{StreamingContext,Duration}

import es.ucm.fdi.sscheck.spark.SharedSparkContext

import scala.util.Try 

trait SharedStreamingContext 
  extends SharedSparkContext {

  /** Override for custom config
  * */
  def batchDuration : Duration 
  
  @transient protected[this] var _ssc : Option[StreamingContext] = None
  def ssc() : StreamingContext = {
    _ssc.getOrElse { 
      // first force the creation of a SparkContext, if needed
      val __sc = sc() 
      logger.warn("creating test Spark Streaming context")
      _ssc = Some(new StreamingContext(__sc, batchDuration))
      _ssc.get
    }
  }
  
  /** Close the shared StreamingContext. NOTE the inherited SparkContext 
   *  is NOT closed, as often we would like to have different life cycles 
   *  for these two kinds of contexts: SparkContext is heavyweight and
   *  StreamingContext is lightweight, and we cannot add additional computations
   *  to a StreamingContext once it has started
   * */
  override def close() : Unit = {
    close(stopSparkContext=false)
  }
  
  /** Close the shared StreamingContext, and optionally the inherited
   *  SparkContext. Note if stopSparkContext is true then the inherited
   *  SparkContext is closed, even if the shared StreamingContext was already closed
   *  */ 
  def close(stopSparkContext : Boolean = false) : Unit = {
     // stop _ssc with stopSparkContext=false and then stop _sc
     // with super[SharedSparkContext].close() if stopSparkContext
     // to respect logging and other additional actions
    _ssc.foreach { ssc =>
      logger.warn("stopping test Spark Streaming context")
      Try {
        ssc.stop(stopSparkContext=false, stopGracefully=true)
      } recover {
        case _ => {
          logger.warn("forcing stop of test Spark Streaming context")
          ssc.stop(stopSparkContext=false, stopGracefully=false)
        }
      }
      _ssc = None
    }
    if (stopSparkContext) {
      super[SharedSparkContext].close()
    }
  }
}