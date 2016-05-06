package es.ucm.fdi.sscheck.spark.streaming

import org.apache.spark.streaming.{StreamingContext,Duration}

import org.slf4j.LoggerFactory

import scala.util.Try 

import es.ucm.fdi.sscheck.spark.SharedSparkContext

trait SharedStreamingContext 
  extends SharedSparkContext {

  // cannot use private[this] due to https://issues.scala-lang.org/browse/SI-8087
  // @transient private[this] val logger = Logger(LoggerFactory.getLogger("SharedStreamingContext"))
  @transient private val logger = LoggerFactory.getLogger("SharedStreamingContext")
  
  /** Override for custom config
  * */
  def batchDuration : Duration 
  
  // disabled by default because it is quite costly
  def enableCheckpointing : Boolean = false 
  
  @transient protected[this] var _ssc : Option[StreamingContext] = None
  def ssc() : StreamingContext = {
    _ssc.getOrElse { 
      // first force the creation of a SparkContext, if needed
      val __sc = sc()
      logger.warn(s"creating test Spark Streaming context")
      _ssc = Some(new StreamingContext(__sc, batchDuration))
      if (enableCheckpointing) {
        val checkpointDir = Utils.createTempDir().toString
        logger.warn(s"configuring Spark Streaming checkpoint directory ${checkpointDir}")
        _ssc.get.checkpoint(checkpointDir)
      }
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
        /* need to use stopGracefully=false for DStreamProp.forAllAlways to work
         * ok in an spec with several props
        */ 
        //ssc.stop(stopSparkContext=false, stopGracefully=true)
        ssc.stop(stopSparkContext=false, stopGracefully=false) 
      } recover {
        case _ => {
          logger.warn("second attempt forcing stop of test Spark Streaming context")
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