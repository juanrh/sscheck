package es.ucm.fdi.sscheck.spark.streaming

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.scalacheck.{Parameters, ScalaCheckProperty}
import org.specs2.specification.BeforeAfterEach
import org.specs2.execute.{AsResult, Result}

import org.scalacheck.{Prop, Gen}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.AnyOperators

import org.apache.spark._
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverStarted, StreamingListenerBatchCompleted}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.Time

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try
import ExecutionContext.Implicits.global

import java.lang.ThreadLocal

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.reflect.{classTag, ClassTag}
import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll
import org.apache.spark.streaming.dstream.DynSeqQueueInputDStream
import es.ucm.fdi.sscheck.{TestCaseIdCounter,PropResult,TestCaseId,TestCaseRecord}

// sbt "test-only es.ucm.fdi.sscheck.spark.streaming.StreamingContextDirectReceiverTest"

/* Implementation notes: TODO move to  the file defining the trait 
 *  
 * # Send parallelism is controlled with the number of workers of the Prop, with one test case 
 * being executed in parallel per worker. Test cases are identified with a id safely generated
 * with an instance of TestCaseIdCounter. Each worker waits for completion of as many batches
 * as the length of its dstream test case. The synchronization needed to wait for batch completion       
 * is obtained by associating a SyncVar to each worker through a shared ThreadLocal[SyncVar[Unit]]. 
 * The initial and only value for that ThreadLocal is defined by overriding initialValue(), which 
 * registers a StreamingListener per worker that onBatchCompleted makes a put() to the SyncVar 
 * if not set, so workers wait with take() before sending a new batch.
 * Alternatives to this mechanism could be:
 * 
 * - java.util.concurrent.LinkedBlockingQueue is another option, the idea is sharing a single
 * LinkedBlockingQueue[Unit] among all the workers and a single StreamingListener. If we know the number
 * of workers then the StreamingListener would put a () per each worker onBatchCompleted, and the workers 
 * would block with take() before sending a new batch. A race condition is possible in this setting, for example with 
 * two workers we could have: 
 * 
 * - worker 1 and worker 2 block waiting in take()
 * - StreamingListener puts 2 () in the queue 
 * - worker 1 completes the take 
 * - worker 1 blocks waiting in a take()
 * - worker 1 completes the take 
 * 
 * So in that situation worker 1 might skip 2 batches, "stealing" a batch from worker 2. The
 * problem is that all the () inserted in the queue are the same, and don't have a recipient address.   
 * 
 * - java.util.concurrent.Phaser or java.util.concurrent.CyclicBarrier are another option, but they are
 * symmetric in the sense that all the parties wait for each other, while in this case we would want 
 * all the workers to wait for a single StreamingListener, which is a fundamentally asymmetric situation 
 * 
 * The solution based on SyncVar is a shared nothing in the synchronization objects, as each worker
 * thread has its private SyncVar and StreamingListener, which should be reflected in decreased 
 * contention that could compensate the overhead of having a SyncVar per worker
 * 
 * # Fail fast
 * Generation of test cases finishes on the first counterexample. propResult is a central point where
 * the assertions for all the workers are checked. propResult starts as None and is assigned to 
 * Some on the first counterexample only. Also:
 * - a test case fails on its first failing batch
 * - a test case fails immediately when it detects other test case at other worker has failed
 * - ScalaCheck stops sending test cases when the first counter example is detected. That means
 * the Prop finishes execution and we arrive to after(), where we can stop the StreamingContext 
 * immediately, because in the Prop we waited until the last batch was completed. We know we won't 
 * run into https://issues.apache.org/jira/browse/SPARK-5681 (which anyway is solved for Spark 1.5.0)
 * because there is no receiver here
 * 
 * # Test case for the counterexample and failing matcher are aligned
 * This is granted by propResult being assigned Some only once, and the use of TestCaseId to
 * identify the test cases
 * */

@RunWith(classOf[JUnitRunner])
class StreamingContextDirectReceiverTest 
  extends org.specs2.Specification 
  with org.specs2.matcher.MustThrownExpectations
  with BeforeAfterEach
  with SharedSparkContextBeforeAfterAll
  with ScalaCheck {
  
  // cannot use private[this] due to https://issues.scala-lang.org/browse/SI-8087
  @transient private val logger = Logger(LoggerFactory.getLogger("StreamingContextDirectReceiverTest"))
    
  override def sparkMaster : String = "local[*]"
  
  var _ssc : Option[StreamingContext] = None
  // with too small batch intervals the local machine just cannot handle the work
  def batchDuration = Duration(800) // Duration(1000) // Duration(500) // Duration(300)  // Duration(10) 
  // def batchDuration = Duration(10)
  
  override def before : Unit = {
    assert(_ssc.isEmpty)
    _ssc = Some(new StreamingContext(sc, batchDuration))
    logger.warn("created test Streaming Context")
  }
  override def after : Unit = {
    assert(! _ssc.isEmpty)
    logger.warn("stopping spark streaming context")
    _ssc. get. stop(stopSparkContext=false, stopGracefully=false)
    _ssc = None
  }
  
  def is = 
    sequential ^
    "Spark Streaming and ScalaCheck tests should" ^
      "use TestCaseDictInputDStream to send data to a dstream"  ! testCaseDictInputDStreamProp
      
  //val dsgenSeqSeq1 = Gen.listOfN(30, Gen.listOfN(50, Gen.choose(1, 100)))
  // for checking race conditions
  val batchSize = 2 // 50 //  5 // 30 
  val zeroSeqSeq = Gen.listOfN(10,  Gen.listOfN(batchSize, 0)) // Gen.listOfN(30,  Gen.listOfN(50, 0))
  val oneSeqSeq = Gen.listOfN(10, Gen.listOfN(batchSize, 1)) // Gen.listOfN(30, Gen.listOfN(50, 1))
  val dsgenSeqSeq1 = Gen.oneOf(zeroSeqSeq, oneSeqSeq)   
       
  def testCaseDictInputDStreamProp = {
    val ssc = _ssc.get
    val inputDStream = // new DynSeqQueueInputDStream[(TestCaseId, Int)](ssc, numSlices = 2)
      new DynSeqQueueInputDStream[TestCaseRecord[Int]](ssc, numSlices = 2)
    
    inputDStream.foreachRDD((rdd, time) => println(s"rdd at time ${time}, size = ${rdd.count}, records = ${rdd.collect.mkString(", ")} "))
  
    // Assertions go here before the streaming context is started
    // TODO: put the assertions in one place, that should see this as it  
    // was a single DStream, instead of a multiplexing
    var i = 0
    var propResult : Option[PropResult] = None 
    inputDStream.foreachRDD { rdd =>
      // NOTE: batch cannot be completed until this code finishes, use
      // future if needed to avoid blocking the batch
      i += 1
      logger.debug(s"found ${i}th batch ") 
      if (rdd.count > 0) {  
        // Note this way we only handle the keys for the test cases 
        // that are currently running  
        // TODO: this could be optimized by mapping Future or using par on rdd.keys.distinct.collect, which 
        // would is small as it has the number of workers as size, and then awaiting for the 
        // results but doing propResult = Some(...) on the first fail. Using a map is nice
        // because then we have as much parallelism as the number of workers
        for (testCaseId <- rdd.keys.distinct.collect) {
          if (! propResult.isDefined) {
            val thisBatchResult = AsResult {
              val batchForThisTestId = rdd.filter(_._1 == testCaseId).flatMap(_._2.toList)                
              logger.warn(s"batchForThisTestId $testCaseId: ${batchForThisTestId.collect.mkString(", ")}")
              batchForThisTestId.count === batchSize
              val batchMean = batchForThisTestId.mean()
              logger.info(s"test ${testCaseId} current batch mean is ${batchMean}")
              List(0.0, 1.0) must contain(batchMean)
              // 0 === 1 // fails ok even if the error is not the last matcher
              // batchMean must be equalTo(1.0) // should fail for some inputs
              batchForThisTestId.distinct.count === 1 // all inputs are the same
            }
            if (thisBatchResult.isFailure || thisBatchResult.isError  || thisBatchResult.isThrownFailure) {
              // TODO: check https://etorreborre.github.io/specs2/guide/SPECS2-3.6.2/org.specs2.guide.StandardResults.html to see
              // this is ok
              // Invariant: propResult is assigned to Some at most once, only for the first counterexample 
              // found. This way we ensure we don't mix errors from different test cases 
              propResult = Some(PropResult(testCaseId = testCaseId, result = thisBatchResult))
            }
          }
        }
      }
    }
   
    ssc.start()
    
    // Note: no wait for receiver, as there is no receiver
    
    // Synchronization stuff
    // test case id counter / generator
    val testCaseIdCounter = new TestCaseIdCounter
    // each worker thread has its own SyncVar with a registered addStreamingListener
    // that notifies onBatchCompleted. Better do this after StreamingContextUtils.awaitUntilReceiverStarted
    // just in case, to await weird race conditions
    val localOnBatchCompletedSyncVar = new ThreadLocal[SyncVar[Unit]] {
      override def initialValue() : SyncVar[Unit]  = { 
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
        onBatchCompletedSyncVar
      }
    } 
        
    val thisProp = AsResult { Prop.forAllNoShrink ("pdstream" |: dsgenSeqSeq1) { testCaseDstream : List[List[Int]] =>
      // here we have a thread per worker
      // no need to synchronize for thread / call stack local variables
      val testCaseId : TestCaseId = testCaseIdCounter.nextId() 
      var testCaseResult : Result = success
      // to stop in the middle of the test case as soon as a counterexample is found 
      // Note: propFailed is not equivalent to propResult.isDefined, because propFailed is
      // only defined after a wait for onBatchCompleted
      var propFailed = false 
      logger.warn(s"starting test case $testCaseId")
      
      // add current test case to inputDStream
      // inputDStream.addDStream(testCaseDstream.map(_.map((testCaseId, _)))) FIXME
      inputDStream.addDStream(testCaseDstream.map({ batch =>
        // Invariant: for each test case and batch, we have either a 
        // single None, or many Some
        if (batch isEmpty) List((testCaseId, None))
        else batch.map(r => (testCaseId, Some(r)))
      }))
      
      for (i <- 1 to testCaseDstream.length if (! propFailed)) {
        // await for the end of the each batch 
        logger.warn(s"waiting end of batch ${i} of test case ${testCaseId} at thread ${Thread.currentThread()}")
        //  FIXME add timeout, owise exceptions in assertions lead to infitive loop
        localOnBatchCompletedSyncVar.get.take() // wait for the SyncVar of this thread
        logger.warn(s"awake after end of batch ${i} of test case ${testCaseId} at thread ${Thread.currentThread()}")        
        if (propResult.isDefined) {
          // some worker generated a counterexample
          propFailed = true
          // is this worker that worker? 
          val somePropResult = propResult.get 
          if (somePropResult.testCaseId == testCaseId) {
            // we are the failing worker, so this is our result
            testCaseResult = somePropResult.result
          }
        }
        // else do nothing, as the data is already sent 
      }
        
      // Note: ScalaCheck will show the correct test case that caused the counterexample
      // because only the worker that generated that counterexample will fail. Anyway we could
      // use testCaseResult.mapMessage here to add testCaseDstream to the message of 
      // testCaseResult if we needed it
      logger.warn(s"finished test case $testCaseId with result $testCaseResult")
      testCaseResult
  }.set(workers = 3, minTestsOk = 10).verbose } // Note: overriding the number of workers works ok
    thisProp 
  }
  
}