package org.apache.spark.streaming.dstream

import org.apache.spark.streaming.{StreamingContext,Time}
import org.apache.spark.streaming.scheduler.InputInfo
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag
import scala.language.postfixOps
import java.io.{NotSerializableException, ObjectOutputStream}

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

/** Parallelization is only performed on compute(), to avoid too many RDDs. This
 *  class is for local testing anyway, and we could easily modify this class to 
 *  enqueue lists of RDDs if we wanted 
 *  
 *  TODO: explore use of par to speed up compute(), with a configurable
 *  execution context / thread pool on contruction
 * */
class DynSeqQueueInputDStream [A:ClassTag](
    @transient val _ssc : StreamingContext,
    val numSlices : Int = 2,
    val verbose : Boolean = false
  ) extends InputDStream[A](_ssc) {
  
  // cannot use private[this] due to https://issues.scala-lang.org/browse/SI-8087
  //@transient private[this] val logger = Logger(LoggerFactory.getLogger("DynSeqQueueInputDStream"))
  @transient private val logger = Logger(LoggerFactory.getLogger("DynSeqQueueInputDStream"))
  
  @transient val _sc = _ssc.sparkContext
  
  /** Each element of this list corresponds to one of the 
   *  DStreams that will be generated
   * */
  private var dstreams : List[List[Seq[A]]] = Nil
  
  private[this] def reset() : Unit = { dstreams = Nil }
    
  private def writeObject(oos: ObjectOutputStream): Unit = {
    throw new NotSerializableException(s"${this.getClass().getName()} doesn't support checkpointing")
  }
  
  def addDStream(dstream : Seq[Seq[A]]) : Unit = synchronized {
    dstreams = dstream.toList :: dstreams
  }
   
  override def start() : Unit = reset()
  override def stop() : Unit = reset()  
  override def compute(validTime: Time): Option[RDD[A]] = synchronized {    
    val dstreamsAndBatch = 
      // foldLeft reverses dstreams but anyway we give no warranty on 
      // the order the records of added DStreams appear in the result, 
      // while foldLeft gives tail recursion
      dstreams.foldLeft((Nil : List[List[Seq[A]]] , Nil : List[A])) {
        case ((inDStreams, inBatch), dstream) => {
          val outBatch = dstream.head.toList ::: inBatch
          val outDStreams = if (dstream.tail isEmpty) inDStreams 
            				else dstream.tail :: inDStreams
          (outDStreams, outBatch)
        } 
      }
    dstreams = dstreamsAndBatch._1
    val batch = dstreamsAndBatch._2
    
    if (verbose) { logger.debug(s"dstreams = $dstreams") }
     
    if (batch.size > 0) {
      logger.debug(s"computing batch ${batch.take(10).mkString(",")}")
      
      // copied from DirectKafkaInputDStream
      // Report the record number of this batch interval to InputInfoTracker.
      val numRecords = batch.length
      val inputInfo = InputInfo(id, numRecords)
      ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    
      val rdd = _sc.parallelize(batch, numSlices=numSlices)
      rdd.count // force compute or this does nothing
      Some(rdd)
    } else {
      None 
    }
  } 
}


object DynSeqQueueInputDStreamOpt {
  /** @return a new instance of DynSeqQueueInputDStreamOpt where all
   *  the None batches have been replaced by empty batches, and all
   *  some records have been projected from Option[A] to A
   */
  def stream[A:ClassTag]
    (ssc : StreamingContext, numSlices : Int, 
     verbose : Boolean = false) : DStream[A] = 
       new DynSeqQueueInputDStreamOpt[A](ssc, numSlices, verbose)
           .flatMap {
             case Some(x) => List(x)
             case None => List()
           }
}
/** Alternative version of DynSeqQueueInputDStreamOpt that 
 *  always emits some data. To do that a singleton batch with the
 *  record None is emitted for empty batches, and batches with Some
 *  are used otherwise. Note this implies that an absent batch cannot
 *  be distinguish from an empty batch. 
 *  
 *  Currently this class is not used in DStreamProp, if it is needed
 *  in the future we could create a new sealed trait with 3 case classes
 *  to distinguish between no batch generated, empty batch and batch with
 *  records. That might make more sense for test case multiplexion 
 */
class DynSeqQueueInputDStreamOpt[A:ClassTag](
    @transient val _ssc : StreamingContext,
    val numSlices : Int = 2,
    val verbose : Boolean = false
  ) extends InputDStream[Option[A]](_ssc) {
  
  @transient private val logger = Logger(LoggerFactory.getLogger("DynSeqQueueInputDStreamOpt"))  
  @transient val _sc = _ssc.sparkContext
  
  /** Each element of this list corresponds to one of the 
   *  DStreams that will be generated
   * */
  private var dstreams : List[List[Seq[A]]] = Nil
  
  private[this] def reset() : Unit = { dstreams = Nil }
    
  private def writeObject(oos: ObjectOutputStream): Unit = {
    throw new NotSerializableException(s"${this.getClass().getName()} doesn't support checkpointing")
  }
  
  def addDStream(dstream : Seq[Seq[A]]) : Unit = synchronized {
    dstreams = dstream.toList :: dstreams
  }
   
  override def start() : Unit = reset()
  override def stop() : Unit = reset()  
  override def compute(validTime: Time): Option[RDD[Option[A]]] = synchronized {    
    val dstreamsAndBatch = 
      // foldLeft reverses dstreams but anyway we give no warranty on 
      // the order the records of added DStreams appear in the result, 
      // while foldLeft gives tail recursion
      dstreams.foldLeft((Nil : List[List[Seq[A]]] , Nil : List[A])) {
        case ((inDStreams, inBatch), dstream) => {
          val outBatch = dstream.head.toList ::: inBatch
          val outDStreams = if (dstream.tail isEmpty) inDStreams 
                    else dstream.tail :: inDStreams
          (outDStreams, outBatch)
        } 
      }
    dstreams = dstreamsAndBatch._1
    val batch = dstreamsAndBatch._2
    
    if (verbose) { logger.debug(s"dstreams = $dstreams") }
     
    if (batch.size > 0) {
      logger.debug(s"computing batch ${batch.take(10).mkString(",")}")
      
      // copied from DirectKafkaInputDStream
      // Report the record number of this batch interval to InputInfoTracker.
      val numRecords = batch.length
      val inputInfo = InputInfo(id, numRecords)
      ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)
    
      val rdd = _sc.parallelize(batch, numSlices=numSlices)
      rdd.count // force compute or this does nothing
      Some(rdd.map(Some(_)))
    } else {
      Some(_sc.parallelize(List(None), 1))
    }
  } 
}