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

import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll
import es.ucm.fdi.sscheck.spark.streaming.receiver.ProxyReceiverActor

/*
 * TODO
 * - block before Prop.forAll until onReceiverStarted, otherwise first batches are
 * lost. Now we have an arbitrary timeout
 * - wait to send data for new batch onBatchCompleted
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
                     with ScalaCheck {
   
  var maybeSsc : Option[StreamingContext] = None
  def batchDuration = Duration(10)
  
  override def before : Unit = {
    assert(maybeSsc.isEmpty)
    maybeSsc = Some(new StreamingContext(sc, batchDuration))
  }
  override def after : Unit = {
    assert(! maybeSsc.isEmpty)
    // TODO: block until all the batches have been processed
    Thread.sleep(2 * 1000) // FIXME this could be a global timeout, or better something that looks if data is being sent or not
                             // maybe be could have a counter of test cases currently running
    println("stopping spark streaming context")
    maybeSsc. get. stop(stopSparkContext=false, stopGracefully=false)
    maybeSsc = None
  }
  
  def is = 
    sequential ^
    "Spark Streaming and ScalaCheck tests should" ^
    "use a proxy actor receiver to send data to a dstream in parallel"  ! actorSendingProp
   
  // val dsgenSeqSeq1 = Gen.listOfN(3, Gen.listOfN(2, Gen.choose(1, 100)))
  val dsgenSeqSeq1 = Gen.listOfN(30, Gen.listOfN(50, Gen.choose(1, 100)))

  def actorSendingProp = {
    println("Creating Streaming Context")
    val ssc = maybeSsc.get
    val receiverActorName = "actorDStream1"
    val (actor1 , actorInputDStream1) = 
      (ProxyReceiverActor.getActorSelection(receiverActorName), 
       ProxyReceiverActor.createActorDStream[Int](ssc, receiverActorName))
    actorInputDStream1.print()
     // TODO: assertions go here before the streaming context is started   
    ssc.start()
    // TODO: block before sending data, if not we have data loss: do it with 
    // the handler, not a timeout
    Thread.sleep(1000)
    val thisProp = Prop.forAll ("pdstream" |: dsgenSeqSeq1) { pdstream : Seq[Seq[Int]] =>
      // TODO: wait for the end of the batch to send more data
      // TODO: use scalacheck callback to get test case id
      pdstream foreach { batch => {
          println(s"Sending to actor $actor1 batch ${batch.mkString(", ")}")
          batch.foreach(actor1 ! _)
        }
      }
      println("sending last message og the test case")
      actor1 ! -42 
      true
    }.set(workers = 5, minTestsOk = 100).verbose

    // Returning thisProperty as the result for this example instead of ok or something like that 
    // is crucial for failing when the prop fails, even in  "thrown expectations" mode of Specs2
    // https://etorreborre.github.io/specs2/guide/SPECS2-3.6.2/org.specs2.guide.Structure.html
    // But this way the property its not executed until the end!, so we cannot stop the streaming
    // context in the property, but in a BeforeAfterEach
    thisProp 
  }
  
}