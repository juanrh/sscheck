package es.ucm.fdi.sscheck.spark.streaming

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.scalacheck.{Parameters, ScalaCheckProperty}
import org.specs2.specification.BeforeAfterEach
import org.specs2.execute.AsResult

import org.scalacheck.{Prop, Gen}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.AnyOperators

import org.apache.spark._
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverStarted, StreamingListenerBatchCompleted}

import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import java.util.concurrent.atomic.AtomicLong
import java.lang.ThreadLocal

import com.typesafe.scalalogging.slf4j.Logging

import akka.actor.ActorSelection
import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll
import es.ucm.fdi.sscheck.spark.streaming.receiver.ProxyReceiverActor

/*
 * TODO
 * - add assertions and fail the property on assertion fail => accumulator?, or exploit foreachRDD running 
 * at the driver wrapping with AsResult? Fail fast and stop streaming context on assertion fail
 * - stop the context only after all the batches for all the test cases have been
 * processed. An approach could be storing the start time and number of batches of 
 * the test case that starts last, in a synchronized variable for the Specification, 
 * so in after() we register a listener that waits until the time of the last completed 
 * batch corresponds to the last batch of the last test case
 * If the context is stopped before the receiver has started then due to 
 * https://issues.apache.org/jira/browse/SPARK-5681 the streaming context will keep running. But
 * that won't happen here because we don't send data until the receiver has started 
 * - test with more than one property: should not be a problem due to sequential. Probably
 * exceptions will be thrown due to https://issues.apache.org/jira/browse/SPARK-8743, but 
 * that is solved and fixed for Spark 1.4.2
 * - encapsulate in a trait for nice code reuse
 * - add some test examples: use TL generators for that?
 * */

/* The send parallelism is controlled with the number of workers of the Prop, with one test case 
 * being executed in parallel per worker. Test cases are identified with a Long id safely generated
 * with an AtomicLong. Synchronization is obtained by associating a SyncVar to each worker through a 
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
 * */

@RunWith(classOf[JUnitRunner])
class StreamingContextActorReceiverTest extends org.specs2.Specification 
                     with org.specs2.matcher.MustThrownExpectations
                     with BeforeAfterEach
                     with SharedSparkContextBeforeAfterAll
                     with ScalaCheck 
                     with Logging {
   
  override def sparkMaster : String = "local[8]"
  
  var maybeSsc : Option[StreamingContext] = None
  // with too small batch intervals the local machine just cannot handle the work
  def batchDuration = Duration(300) // Duration(500) // Duration(10)
  // def batchDuration = Duration(10)
  
  override def before : Unit = {
    assert(maybeSsc.isEmpty)
    maybeSsc = Some(new StreamingContext(sc, batchDuration))
    logWarning("created test Streaming Context")
  }
  override def after : Unit = {
    assert(! maybeSsc.isEmpty)
    // TODO: block until all the batches have been processed: could 
    // send a signal in the prop after the last test case. Or maybe 
    // consider Around and AroundTimeout https://etorreborre.github.io/specs2/guide/SPECS2-3.6.2/org.specs2.guide.TimeoutExamples.html
    Thread.sleep(20 * 1000) // FIXME this could be a global timeout, or better something that looks if data is being sent or not
                             // maybe be could have a counter of test cases currently running
    logWarning("stopping spark streaming context")
    maybeSsc. get. stop(stopSparkContext=false, stopGracefully=false)
    maybeSsc = None
  }
  
  def is = 
    sequential ^
    "Spark Streaming and ScalaCheck tests should" ^
      "use a proxy actor receiver to send data to a dstream in parallel"  ! actorSendingProp
   
     // val dsgenSeqSeq1 = Gen.listOfN(3, Gen.listOfN(2, Gen.choose(1, 100)))
     // val dsgenSeqSeq1 = Gen.listOfN(50, Gen.listOfN(30, Gen.choose(1, 100)))
    //val dsgenSeqSeq1 = Gen.listOfN(30, Gen.listOfN(50, Gen.choose(1, 100)))
    // for checking race conditions
    val zeroSeqSeq = Gen.listOfN(50,  Gen.listOfN(30, 0))
    val oneSeqSeq = Gen.listOfN(50, Gen.listOfN(30, 1))
    val dsgenSeqSeq1 = Gen.oneOf(zeroSeqSeq, oneSeqSeq)   
    
  def actorSendingProp = {
    val ssc = maybeSsc.get
    val receiverActorName = "actorDStream1"
    val (proxyReceiverActor , actorInputDStream) = 
      (ProxyReceiverActor.getActorSelection(receiverActorName), 
       ProxyReceiverActor.createActorDStream[(Long, Int)](ssc, receiverActorName))
    actorInputDStream.print()
    var i = 0
    actorInputDStream.foreachRDD { rdd =>
      // NOTE: batch cannot be completed until this code finishes, use
      // future if needed to avoid blocking the batch
      i += 1
      logDebug(s"found ${i}th batch ")
      // TODO: for now it is normal that the mean is mixed, as test 
      // cases are nor separated by key
      if (rdd.count > 0) {  
        // note this way we only handle the keys for the test cases 
        // that are currently running  
        for (key <- rdd.keys.distinct.toLocalIterator) {
          val valsForThisKey = rdd.filter(_._1 == key).values
          val rddMean = valsForThisKey.mean()
          List(0.0, 1.0) must contain(rddMean)
          valsForThisKey.distinct.count === 1
          // 0 === 1 TODO should fail on this
          logDebug(s"mean for key ${key}= ${valsForThisKey.mean()}")
        }
      }
    }
    // TODO: assertions go here before the streaming context is started
    ssc.start()
    
    // wait for the receiver to start before sending data, otherwise the 
    // first batches are lost because we are using ! to send the data to the actor
    StreamingContextUtils.awaitUntilReceiverStarted(ssc, atMost = 5 seconds)
    logWarning("receiver has started")
    
    // test case id counter / generator
    val testCaseIdCounter = new AtomicLong(0)
    // each worker thread has its own SyncVar with a registered addStreamingListener
    // that notifies onBatchCompleted
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

    // using AsResult explicitly to be independent from issue #393
    val thisProp = AsResult { Prop.forAll ("pdstream" |: dsgenSeqSeq1) { prefixDstream : Seq[Seq[Int]] =>
      // here we have a thread per worker
      // no need to synchronize for thread / call stack local variables
      val testCaseId = testCaseIdCounter.getAndIncrement()
      logWarning(s"starting test case $testCaseId")
      for (batch <- prefixDstream) {
        // await for the end of a new batch to send more data
    	logInfo(s"waiting for batch end at thread ${Thread.currentThread()}")
        localOnBatchCompletedSyncVar.get.take() // wait for the SyncVar of this thread 
        logInfo(s"awake after batch end at thread ${Thread.currentThread()}")        
        logDebug(s"sending to proxy actor $proxyReceiverActor new batch ${batch.mkString(", ")}")
        batch. foreach(proxyReceiverActor ! (testCaseId, _))
      }
      logWarning(s"finished test case $testCaseId")
            
      // TODO: check sync of data sent with how data is checked: i.e. alignment of data
      // production and assertions. I think we get that from Spark if all the 
      // checks are performed as actions
      true
    }.set(workers = 6, minTestsOk = 100).verbose // NOTE: we always use 1 worker, and leave parallel execution to Spark => override user settings for that
  }

    // Returning thisProperty as the result for this example instead of ok or something like that 
    // is crucial for failing when the prop fails, even in  "thrown expectations" mode of Specs2
    // https://etorreborre.github.io/specs2/guide/SPECS2-3.6.2/org.specs2.guide.Structure.html
    // But this way the property its not executed until the end!, so we cannot stop the streaming
    // context in the property, but in a BeforeAfterEach
    thisProp 
  }
  
}