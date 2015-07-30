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

import scala.concurrent._
import scala.concurrent.duration._
import scala.util.Try
import ExecutionContext.Implicits.global
import java.util.concurrent.atomic.AtomicInteger
import java.lang.ThreadLocal

import com.typesafe.scalalogging.slf4j.Logging

import akka.actor.ActorSelection
import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll
import es.ucm.fdi.sscheck.spark.streaming.receiver.ProxyReceiverActor

/*
 * FIXME:
 * - support empty batches with Either: currently we cannot create an empty batch 
 * because we emit nothing for empty batches. So replace multiplexing DStream[(TestId, A)] by
 * DStream[(TestId, Either[A])] where Left is used for empty batches, and Right 
 * for non empty batches. We maintain an invariant that in the multiplexing DStream
 * for each key we either have one or more Right, or a single Left 
 * 
 * TODO

 * - create derived dstreams from the generated dstream, and assert on them, abstracting away from 
 * test multiplexion: try with a derived dstream for map(_+1) and adapt the assertions
 * - test with more than one property: should not be a problem due to sequential. Probably
 * exceptions will be thrown due to https://issues.apache.org/jira/browse/SPARK-8743, but 
 * that is solved and fixed for Spark 1.4.2
 * - encapsulate in a trait for nice code reuse. Consider Shapeless HList https://github.com/milessabin/shapeless/wiki/Feature-overview:-shapeless-2.0.0 
 * to simulate heterogeneously typed varargs, instead of relaying on several overload for different arities. But
 * it is important that the user only needs to use raw tuples, use the typpe reconstruction capabitilies of .cast, or
 * "facilities for abstracting over arity": we can ask the user to use a convention based on position of the argument, 
 * but the type should be preserved. Copy "Implementation notes" below in the file defining the trait 
 * - add some test examples: use TL generators for that?
 * - shrinking is currently not supported
 * */

/* Implementation notes:
 *  
 * # Send parallelism is controlled with the number of workers of the Prop, with one test case 
 * being executed in parallel per worker. Test cases are identified with a Int id safely generated
 * with an AtomicInteger. Synchronization is obtained by associating a SyncVar to each worker through a 
 * shared ThreadLocal[SyncVar[Unit]]. The initial and only value for that ThreadLocal is defined by 
 * overriding initialValue(), which registers a StreamingListener per worker that onBatchCompleted 
 * makes a put() to the SyncVar if not set, so workers wait with take() before sending a new batch. 
 * Alternatives to this mechanism could be:
 * 
 * - java.util.concurrent.LinkedBlockingQueue is another option, the idea is sharing a single
 * LinkedBlockingQueue[Unit] among all the workers and a single StreamingListener. If we know the number
 * of workers then the StreamingListener would put a () per each worker onBatchCompleted, and the workers 
 * would block with take() before sending a new batch. A race condition is possible if sending a batch is
 * too fast for a particular worker. For example with two workers we could have: 
 * 
 * - worker 1 and worker 2 block waiting in take()
 * - StreamingListener puts 2 () in the queue 
 * - worker 1 completes the take and then sends all the records for the current batch 
 * - worker 1 blocks waiting in a take()
 * - worker 1 completes the take and then sends all the records for the next batch
 * 
 * So in that situation worker 1 might send 2 batches, "stealing" a batch from worker 2, because worker 1
 * is able to send all the records for a batch and start a new take() before worker 2 reads the take. The
 * problem is that all the () inserted in the queue are the same, and don't have a recipient address.   
 * As the size of the batches is random, because batches are generated randomly, this is a non trivial problem,
 * and adding a call to Thread.sleep after sending each new batch would be a waste of execution time  
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
 * because we don't start the Prop until the receiver has started, by using 
 * StreamingContextUtils.awaitUntilReceiverStarted, which is needed anyway to avoid losing the 
 * first batches, because we are using ! to send to a ProxyReceiverActor
 * 
 * # Test case for the counterexample and failing matcher are aligned
 * This is granted by propResult being assigned Some only once, and the use of TestCaseId to
 * identify the test cases
 *  
 * # Former race condition
 * This results on a lost of completeness, not of soundness of 
 * the counterexample, which is always a valid counterexample when found. The problem is
 * this possible trace
 * 
 *  . batch 0 completes
 *  . a new test case 1 starts at worker1 and its first and only batch b1 is sent
 *  . worker1 doesn't block for onBatchCompleted, because the test case has finished: then worker 1 
 *  sees that testResult is not a failure, and begins test case 2, and blocks for onBatchCompleted for batch 1  
 *  . batch 1 starts, processing the data from test case 1
 *  . testResult is set to fail for test case 1 when processing test case 1 during batch 1. This means testResult  
 *  will be constant until the end of the property. But worker1 is already in test case 2, so it will never notice,
 *  so the prop will succeed and will never find a counterexample. 
 *  As a possible solution, currently we have for each test case
 *  
 *  for each batch
 *    wait for onBatchCompleted
 *    send data
 *  if testResult failed with this test case:
 *     fail with testResult
 *  else 
 *    succeed
 *  
 *  if we added an additional batch for the last batch, and even checked the error at each batch
 *  
 *  for each batch if not testCaseFailed
 *    wait for onBatchCompleted (for the batch previous to this one)
 *    if testResult failed with this test case: 
 *      testCaseFailed = True
 *    else 
 *      send data
 *  wait for onBatchCompleted (for last batch)
 *  if testCaseFailed: 
 *     fail with testResult
 *  else:
 *    if testResult failed with this test case (for last batch) 
 *      fail with testResult
 *    else: 
 *      succeed
 *      
 * This probably could have problems if batches are too slow due to lack of resources, or a 
 * badly configured batch interval. But in that case we would be in a bad situation to test 
 * anything anyway
 * */

object TestCaseIdCounter {
  type TestCaseId = Int
}
/** This class can be used to generate unique test case identifiers per JVM. 
 * */
class TestCaseIdCounter {
  import TestCaseIdCounter.TestCaseId
  val counter = new AtomicInteger(0)
  /** @return a fresh TestCaseId. This method is thread safe 
   * */
  def nextId() : TestCaseId = counter.getAndIncrement() 
}

/** This class can be used to wrap the result of the execution of a Prop
 * @param testCaseId id of the test case that produced this result 
 * @param result result of the test
 * */
case class PropResult(testCaseId : TestCaseIdCounter.TestCaseId, result : Result)

@RunWith(classOf[JUnitRunner])
class StreamingContextActorReceiverTest extends org.specs2.Specification 
                     with org.specs2.matcher.MustThrownExpectations
                     with BeforeAfterEach
                     with SharedSparkContextBeforeAfterAll
                     with ScalaCheck 
                     with Logging {
  import TestCaseIdCounter.TestCaseId
  
  override def sparkMaster : String = "local[8]"
  
  var _ssc : Option[StreamingContext] = None
  // with too small batch intervals the local machine just cannot handle the work
  def batchDuration = Duration(300) // Duration(500) // Duration(10) 
  // def batchDuration = Duration(10)
  
  override def before : Unit = {
    assert(_ssc.isEmpty)
    _ssc = Some(new StreamingContext(sc, batchDuration))
    logWarning("created test Streaming Context")
  }
  override def after : Unit = {
    assert(! _ssc.isEmpty)
    logWarning("stopping spark streaming context")
    _ssc. get. stop(stopSparkContext=false, stopGracefully=false)
    _ssc = None
  }
  
  def is = 
    sequential ^
    "Spark Streaming and ScalaCheck tests should" ^
      "use a proxy actor receiver to send data to a dstream in parallel"  ! actorSendingProp
   
    //val dsgenSeqSeq1 = Gen.listOfN(30, Gen.listOfN(50, Gen.choose(1, 100)))
    // for checking race conditions
    val batchSize = 5 // 30 
    val zeroSeqSeq = Gen.listOfN(10,  Gen.listOfN(batchSize, 0)) // Gen.listOfN(30,  Gen.listOfN(50, 0))
    val oneSeqSeq = Gen.listOfN(10, Gen.listOfN(batchSize, 1)) // Gen.listOfN(30, Gen.listOfN(50, 1))
    val dsgenSeqSeq1 = Gen.oneOf(zeroSeqSeq, oneSeqSeq)   
        
  def actorSendingProp = {
    val ssc = _ssc.get
    val receiverActorName = "actorDStream1"
    val (proxyReceiverActor , actorInputDStream) = 
      (ProxyReceiverActor.getActorSelection(receiverActorName), 
       ProxyReceiverActor.createActorDStream[(TestCaseId, Int)](ssc, receiverActorName))
    // not really necessary but useful
    actorInputDStream.print()
    
    // Assertions go here before the streaming context is started
    // TODO: put the assertions in one place, that should see this as it  
    // was a single DStream, instead of a multiplexing
    var i = 0
    var propResult : Option[PropResult] = None 
    actorInputDStream.foreachRDD { rdd =>
      // NOTE: batch cannot be completed until this code finishes, use
      // future if needed to avoid blocking the batch
      i += 1
      logDebug(s"found ${i}th batch ") 
      if (rdd.count > 0) {  
        // Note this way we only handle the keys for the test cases 
        // that are currently running  
        // TODO: this could be optimized by mapping Future on rdd.keys.distinct.collect, which 
        // would is small as it has the number of workers as size, and then awaiting for the 
        // results but doing propResult = Some(...) on the first fail. Using a map is nice
        // because then we have as much parallelism as the number of workers
        for (testCaseId <- rdd.keys.distinct.toLocalIterator) {
          if (! propResult.isDefined) {
            val thisBatchResult = AsResult {
              val batchForThisTestId = rdd.filter(_._1 == testCaseId).values
              // batchForThisTestId.count === batchSize // this fails because batches are mixing
              val batchMean = batchForThisTestId.mean()
              logInfo(s"test ${testCaseId} current batch mean is ${batchMean}")
              List(0.0, 1.0) must contain(batchMean)
              // 0 === 1 // fails ok even if the error is not the last matcher
              // batchMean must be equalTo(1.0) // should fail for some inputs
              batchForThisTestId.distinct.count === 1
            }
            if (thisBatchResult.isFailure || thisBatchResult.isError  || thisBatchResult.isThrownFailure) {
              // Invariant: propResult is assigned to Some at most once, only for the first counterexample 
              // found. This way we ensure we don't mix errors from different test cases 
              propResult = Some(PropResult(testCaseId = testCaseId, result = thisBatchResult))
            }
          }
        }
      }
    }
   
    ssc.start()
    
    // wait for the receiver to start before sending data, otherwise the 
    // first batches are lost because we are using ! to send the data to the actor
    StreamingContextUtils.awaitUntilReceiverStarted(ssc, atMost = 5 seconds)
    logWarning("receiver has started")
    
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

    // using AsResult explicitly to be independent from issue #393 for Specs2
    	// TODO: shrinking is currently not supported
    val thisProp = AsResult { Prop.forAllNoShrink ("pdstream" |: dsgenSeqSeq1) { testCaseDstream : Seq[Seq[Int]] =>
      // here we have a thread per worker
      // no need to synchronize for thread / call stack local variables
      val testCaseId : TestCaseId = testCaseIdCounter.nextId() 
      var testCaseResult : Result = success
      // to stop in the middle of the test case as soon as a counterexample is found 
      var propFailed = false 
      logInfo(s"starting test case $testCaseId")
      for (batch <- testCaseDstream if (! propFailed)) {
        // await for the end of a the previous batch 
        logDebug(s"waiting for batch end at thread ${Thread.currentThread()}")
        localOnBatchCompletedSyncVar.get.take() // wait for the SyncVar of this thread 
        logDebug(s"awake after batch end at thread ${Thread.currentThread()}")        
        if (propResult.isDefined) {
          // some worker generated a counterexample
          propFailed = true
          // is this worker that worker? 
          val somePropResult = propResult.get 
          if (somePropResult.testCaseId == testCaseId) {
            // we are the failing worker, so this is our result
            testCaseResult = somePropResult.result
          }
        } else {
           // send data for the current batch
           batch. foreach(proxyReceiverActor ! (testCaseId, _))
           logDebug(s"sending to proxy actor $proxyReceiverActor new batch ${batch.mkString(", ")}")
        }
      }
      // Note: propFailed is not equivalent to propResult.isDefined, because propFailed is
      // only defined after a wait for onBatchCompleted
      if (!propFailed) {
        // If propFailed is false then wait for one more batch, to cover the 
        // case were the last batch of this test case is causing the counterexample
        localOnBatchCompletedSyncVar.get.take() // wait for the SyncVar of this thread
        // check if this was the test case causing the counterexample, wait for that
        // batch to be processed
        if (propResult.isDefined && propResult.get.testCaseId == testCaseId) {
          // we are the failing worker, so this is our result
          testCaseResult = propResult.get.result
        } 
      }
     
      // Note: ScalaCheck will show the correct test case that caused the counterexample
      // because only the worker that generated that counterexample will fail. Anyway we could
      // use testCaseResult.mapMessage here to add testCaseDstream to the message of 
      // testCaseResult if we needed it
      logInfo(s"finished test case $testCaseId with result $testCaseResult")
      testCaseResult
    }.set(workers = 3, minTestsOk = 10).verbose 
  }

    // Returning thisProperty as the result for this example instead of ok or something like that 
    // is crucial for failing when the prop fails, even in  "thrown expectations" mode of Specs2
    // https://etorreborre.github.io/specs2/guide/SPECS2-3.6.2/org.specs2.guide.Structure.html
    // But this way the property its not executed until the end!, so we cannot stop the streaming
    // context in the property, but in a BeforeAfterEach
     thisProp
  }
  
}