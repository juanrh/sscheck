package es.ucm.fdi.sscheck.spark.streaming

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.scalacheck.{Parameters, ScalaCheckProperty}
import org.specs2.specification.BeforeAfterEach

import org.scalacheck.{Prop, Gen}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.AnyOperators

import org.apache.spark._
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerReceiverStarted, StreamingListenerBatchCompleted}

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.concurrent.duration._

import com.typesafe.scalalogging.slf4j.Logging

import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll
import es.ucm.fdi.sscheck.spark.streaming.receiver.ProxyReceiverActor

/*
 * TODO
 * - wait to send data for new batch onBatchCompleted
 * - identify each test case: try to use the test id
 * - add assertions and collect their exceptions with an accumulator
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

@RunWith(classOf[JUnitRunner])
class StreamingContextActorReceiverTest extends org.specs2.Specification 
                     with org.specs2.matcher.MustThrownExpectations
                     with BeforeAfterEach
                     with SharedSparkContextBeforeAfterAll
                     with ScalaCheck 
                     with Logging {
   
  override def sparkMaster : String = "local[5]"
  
  var maybeSsc : Option[StreamingContext] = None
  // with too small batch intervals the local machine just cannot handle the work
  def batchDuration = Duration(300) // Duration(500) // Duration(10)
  
  override def before : Unit = {
    assert(maybeSsc.isEmpty)
    maybeSsc = Some(new StreamingContext(sc, batchDuration))
  }
  override def after : Unit = {
    assert(! maybeSsc.isEmpty)
    // TODO: block until all the batches have been processed
    Thread.sleep(20 * 1000) // FIXME this could be a global timeout, or better something that looks if data is being sent or not
                             // maybe be could have a counter of test cases currently running
    logger.warn("stopping spark streaming context")
    maybeSsc. get. stop(stopSparkContext=false, stopGracefully=false)
    maybeSsc = None
  }
  
  def is = 
    sequential ^
    "Spark Streaming and ScalaCheck tests should" ^
    "use a proxy actor receiver to send data to a dstream in parallel"  ! actorSendingProp
   
  // val dsgenSeqSeq1 = Gen.listOfN(3, Gen.listOfN(2, Gen.choose(1, 100)))
    val dsgenSeqSeq1 = Gen.listOfN(50, Gen.listOfN(30, Gen.choose(1, 100)))
  //val dsgenSeqSeq1 = Gen.listOfN(30, Gen.listOfN(50, Gen.choose(1, 100)))

  def actorSendingProp = {
    logger.warn("creating Streaming Context")
    val ssc = maybeSsc.get
    val receiverActorName = "actorDStream1"
    val (proxyReceiverActor , actorInputDStream) = 
      (ProxyReceiverActor.getActorSelection(receiverActorName), 
       ProxyReceiverActor.createActorDStream[Int](ssc, receiverActorName))
    // actorInputDStream.print()
    actorInputDStream.map(_+1).map(x => (x % 5, 1)).reduceByKey(_+_).print()
    
    // TODO: assertions go here before the streaming context is started
    
    ssc.start()
    
    // wait for the receiver to start before sending data, otherwise the 
    // first batches are lost because we are using ! to send the data to the actor
    StreamingContextUtils.awaitUntilReceiverStarted(ssc, atMost = 5 seconds)
    logger.info("detected that receiver has started")
    
    val thisProp = Prop.forAll ("pdstream" |: dsgenSeqSeq1) { pdstream : Seq[Seq[Int]] =>
      // await for the end of a batch to send more data
        // NOTE this implies one listener per test case, so send is multiplexed already
        // TODO: we could count here how many listeners are registered and not register more
        // at least until some of them have stopped sending data. That doesn't unregister listeners 
        // but limits the amount of work for Spark. A simple approach is waiting for an empty
        // batch, based on a listener that looks on the number of records in BatchInfo
      ssc.addStreamingListener(new StreamingListener {
        var batches = pdstream 
        override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) : Unit =  {
          if (batches.length > 0) {
            val batch = batches(0)
            batches = batches.tail
            // FIXME: use debug instead
            logger.info(s"sending to proxy actor $proxyReceiverActor new batch ${batch.mkString(", ")}")
            batch. foreach(proxyReceiverActor ! _)
          }
        }
      })
      
      // TODO: use scalacheck callback to get test case id
//      pdstream foreach { batch => {
//          // logger.debug FIXME restore
//          logger.info(s"Sending to actor proxyReceiverActor batch ${batch.mkString(", ")}")
//          batch.foreach(proxyReceiverActor ! _)
//        }
//      }
//      logger.info("sending last message of the test case")
//      proxyReceiverActor ! -42 
      true
    }.set(workers = 1, minTestsOk = 50).verbose
    //.set(workers = 5, minTestsOk = 100).verbose FIXME restore

    // Returning thisProperty as the result for this example instead of ok or something like that 
    // is crucial for failing when the prop fails, even in  "thrown expectations" mode of Specs2
    // https://etorreborre.github.io/specs2/guide/SPECS2-3.6.2/org.specs2.guide.Structure.html
    // But this way the property its not executed until the end!, so we cannot stop the streaming
    // context in the property, but in a BeforeAfterEach
    thisProp 
  }
  
}