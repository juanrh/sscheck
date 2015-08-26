package es.ucm.fdi.sscheck.prop

import org.scalacheck.{Properties, Gen}
import org.scalacheck.Prop.{forAll, exists, AnyOperators}
import org.scalacheck.Prop
import org.scalacheck.util.Pretty

import org.specs2.matcher.MatchFailureException
import org.specs2.execute.{AsResult,Result}
import org.specs2.execute.StandardResults
import org.specs2.scalacheck.AsResultProp

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import scala.reflect.ClassTag
import scala.concurrent.SyncVar
import scala.util.{Try, Success, Failure}

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.Properties.lineSeparator

import org.apache.spark.streaming.dstream.DynSeqQueueInputDStream
import es.ucm.fdi.sscheck.spark.Parallelism
import es.ucm.fdi.sscheck.{TestCaseIdCounter,PropResult,TestCaseId,TestCaseRecord}
  
class PropExecutionException(msg : String)
  extends RuntimeException(msg) 
case class TestCaseTimeoutException(msg : String)
  extends PropExecutionException(msg)

object DStreamProp {
  @transient private val logger = Logger(LoggerFactory.getLogger("DStreamProp"))
  
  // Constants used for printing a sample of the generated values for each batch
  val msgHeader = "-"*43
  val numSampleRecords = 4
  
  /** Define a ScalaCheck property for 1 input dstream and 1 transformed dstream, 
   *  that succeeds iff all the assertions hold for each batch
   *  
   * NOTE: currently only supports a single ScalaCheck worker
   * */
  def forAllAlways[E1:ClassTag,E2:ClassTag,P]
    (g1: Gen[Seq[Seq[E1]]])(gt1 : (DStream[E1]) => DStream[E2])(assertions: (RDD[E1], RDD[E2]) => P)
    (implicit pv: P => Prop, rv : P => Result, pp1: Seq[Seq[E1]] => Pretty, 
                            ssc : StreamingContext, parallelism : Parallelism): Prop = {
    
    val inputDStream1 = new DynSeqQueueInputDStream[E1](ssc, numSlices = parallelism.numSlices)
    inputDStream1.foreachRDD { (rdd, time) => 
      println(s"""${msgHeader}
Time: ${time} - InputDStream1 (${rdd.count} records)
${msgHeader}
${rdd.take(numSampleRecords).mkString(lineSeparator)}
...""")
    }
    val transformedStream1 = gt1(inputDStream1) 
    // note we access transformedStream1 but only in slice(), so we need some trivial action
    // on transformedStream1 or we get org.apache.spark.SparkException: org.apache.spark.streaming.dstream.MappedDStream@459bd6af has not been initialized)
    transformedStream1.foreachRDD {rdd => {}}
    
    var propResult : Option[Result] = None     
    
    inputDStream1.foreachRDD { (input1Batch, time) =>
      // NOTE: batch cannot be completed until this code finishes, use
      // future if needed to avoid blocking the batch
      if ((! propResult.isDefined) && input1Batch.count > 0) {  
//        val thisBatchResult = {
//          val trans1Batch = transformedStream1.slice(time, time).head
//          AsResult { assertions(input1Batch, trans1Batch) }
//        }
        val thisBatchResult = {
          val trans1Batches = transformedStream1.slice(time, time)
          if (trans1Batches.length > 0) {
            AsResult { assertions(input1Batch, trans1Batches.head) }
          } else {
            StandardResults.success 
          }
        }
        
        if (thisBatchResult.isFailure || thisBatchResult.isError || thisBatchResult.isThrownFailure) {
          // Checking according to https://etorreborre.github.io/specs2/guide/SPECS2-3.6.2/org.specs2.guide.StandardResults.html 
          // Invariant: propResult is assigned to Some at most once, only for the first counterexample 
          // found. This way we ensure we don't mix errors from different test cases 
          propResult = Some(thisBatchResult)
        }
      }
    }
    
    ssc.start()
    
    // Synchronization stuff
    // test case id counter / generator
    val testCaseIdCounter = new TestCaseIdCounter
    // won't wait for each batch for more than batchCompletionTimeout milliseconds
    val batchInterval = inputDStream1.slideDuration.milliseconds
    val batchCompletionTimeout = batchInterval * 1000 // give a good margin, values like 5 lead to spurious errors
    // the worker thread uses the SyncVar with a registered addStreamingListener
    // that notifies onBatchCompleted.
    // Note: no need to wait for receiver, as there is no receiver
    val onBatchCompletedSyncVar = new SyncVar[Unit]
    ssc.addStreamingListener(new StreamingListener {
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) : Unit =  {
        // signal the property about the completion of a new batch  
        if (! onBatchCompletedSyncVar.isSet) {
          // note only this threads makes puts, so no problem with concurrency
          onBatchCompletedSyncVar.put(())
        }
      }
    })
    
    Prop.forAllNoShrink (g1) { (testCaseDstream : Seq[Seq[E1]]) =>
      val testCaseId : TestCaseId = testCaseIdCounter.nextId() 
      var testCaseResult : Result = StandardResults.success
      // to stop in the middle of the test case as soon as a counterexample is found 
      // Note: propFailed is not equivalent to propResult.isDefined, because propFailed is
      // only defined after a wait for onBatchCompleted
      var propFailed = false 
      logger.warn(s"starting test case $testCaseId")
      
      // add current test case to inputDStream
      // val inputDStream1 = new DynSeqQueueInputDStream[E1](ssc, numSlices = parallelism.numSlices)
      inputDStream1.addDStream(testCaseDstream)
      
      for (i <- 1 to testCaseDstream.length if (! propFailed)) {
        // await for the end of the each batch 
        logger.debug(s"waiting end of batch ${i} of test case ${testCaseId} at thread ${Thread.currentThread()}")
          // use the SyncVar to wait for batch completion
        // onBatchCompletedSyncVar.take()
        Try {
          onBatchCompletedSyncVar.take(batchCompletionTimeout)
        } match {
          case Success(_) => {}
          case Failure(_) => {
            val msg = s"Timeout to complete batch after ${batchCompletionTimeout} ms, expected batch intervarl was ${batchInterval} ms"
            logger.error(msg)
            Try { ssc.stop(stopSparkContext = false, stopGracefully = false) }
            // This exception will make the test case fail, in this case the 
            // failing test case is not important as this is a performance problem, not 
            // a counterexample that has been found
            throw TestCaseTimeoutException(msg)
          }
        }      
        logger.debug(s"awake after end of batch ${i} of test case ${testCaseId} at thread ${Thread.currentThread()}")        
        if (propResult.isDefined) {
          // some batch generated a counterexample
          propFailed = true  
          testCaseResult = propResult.get
          Try { ssc.stop(stopSparkContext = false, stopGracefully = false) }
        }
        // else do nothing, as the data is already sent 
      }
        
      // Note: ScalaCheck will show the correct test case that caused the counterexample
      // because only the test case generated that counterexample will fail. Anyway we could
      // use testCaseResult.mapMessage here to add testCaseDstream to the message of 
      // testCaseResult if we needed it
      logger.warn(s"finished test case $testCaseId with result $testCaseResult")
      AsResultProp.asResultToProp(testCaseResult)
    } 
  }
}
