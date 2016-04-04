package es.ucm.fdi.sscheck.prop.tl

import org.scalacheck.Gen
import org.scalacheck.Prop
import org.scalacheck.util.Pretty

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import scala.reflect.ClassTag
import scala.concurrent.SyncVar
import scala.util.{Try, Success, Failure}

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.Properties.lineSeparator

import es.ucm.fdi.sscheck.{TestCaseIdCounter,TestCaseId}
import es.ucm.fdi.sscheck.spark.{SharedSparkContextBeforeAfterAll,Parallelism}
import es.ucm.fdi.sscheck.spark.streaming
import es.ucm.fdi.sscheck.spark.streaming.TestInputStream

object DStreamTLProperty {
    @transient private val logger = Logger(LoggerFactory.getLogger("DStreamTLProperty"))
}

trait DStreamTLProperty 
  extends SharedSparkContextBeforeAfterAll {
  
  import DStreamTLProperty.{logger}
  
  /** Override for custom configuration
  * */
  def batchDuration : Duration 
  
  /** Override for custom configuration
  *  Disabled by default because it is quite costly
  * */
  def enableCheckpointing : Boolean = false 
  
  /** @return a newly created streaming context, for which no DStream or action has 
   *  been defined, and that it's not started
   *  
   *  Precondition: no streaming context is currently started
   *  in this JVM
   * */
  def buildFreshStreamingContext() : StreamingContext = {
    val __sc = sc()
    logger.warn(s"creating test Spark Streaming context")
    val newSsc = new StreamingContext(__sc, batchDuration)
    if (enableCheckpointing) {
        val checkpointDir = streaming.Utils.createTempDir().toString
        logger.warn(s"configuring Spark Streaming checkpoint directory ${checkpointDir}")
        newSsc.checkpoint(checkpointDir)
      }
    newSsc
  }
    
  /** @return a ScalaCheck property that is executed by: 
   *  - generating a prefix of a DStream with g1
   *  - generating a derived DStream with gt1
   *  - checking formula on those DStreams
   *  
   *  The property is satisfied iff all the test cases satisfy the formula. 
   *  A new streaming context is created for each test case to isolate its
   *  execution, which is particularly relevant if gt1 is stateful 
   *  
   *  WARNING: the resulting Prop cannot be configured for parallel execution of 
   *  test cases, in order to avoid having more than a single StreamingContext 
   *  started in the same JVM 
   * */
  def forAllDStream[E1:ClassTag,E2:ClassTag]
            (g1: Gen[Seq[Seq[E1]]])(gt1 : (DStream[E1]) => DStream[E2])
            (formula : Formula[(RDD[E1], RDD[E2])])
            (implicit pp1: Seq[Seq[E1]] => Pretty) : Prop = {  
    val formulaNext = formula.nextFormula
    // test case id counter / generator
    val testCaseIdCounter = new TestCaseIdCounter

    // Create a new streaming context per test case, and use it to create a new TestCaseContext
    // that will use TestInputStream from spark-testing-base to create new input and output 
    // dstreams, and register a foreachRDD action to evaluate the formula
    Prop.forAllNoShrink (g1) { (testCaseDstream : Seq[Seq[E1]]) =>
      // Setup new test case
      val testCaseId : TestCaseId = testCaseIdCounter.nextId() 
      // create, start and stop context for each test case      
        // create a fresh streaming context for this test case, and pass it unstarted to 
        // a new TestCaseContext, which will setup the streams and actions, and start the streaming context
      val freshSsc = buildFreshStreamingContext() 
      val testCaseContext = new TestCaseContext[E1, E2](testCaseDstream, gt1, formulaNext)(freshSsc, parallelism)
        // we use propFailed to stop in the middle of the test case as soon as a counterexample is found 
        // Note: propFailed is not equivalent to currFormula.result.isDefined, because propFailed is
        // only defined after a wait for onBatchCompleted
      var propFailed = false 
      logger.warn(s"starting test case $testCaseId")      
      for (i <- 1 to testCaseDstream.length if (! propFailed)) {
        // wait for batch completion
        logger.debug(s"waiting end of batch ${i} of test case ${testCaseId} at ${Thread.currentThread()}")
        testCaseContext.waitForBatch()
        logger.debug(s"awake after end of batch ${i} of test case ${testCaseId} at ${Thread.currentThread()}")         
        if (testCaseContext.currFormula.result.isDefined && {
            val currFormulaResult = testCaseContext.currFormula.result.get
            currFormulaResult == Prop.False || currFormulaResult.isInstanceOf[Prop.Exception]
            }) {  
          // some batch generated a counterexample
          propFailed = true  
        }
        // else do nothing, as the data is already sent 
      }
      // the execution of this test case is completed
      testCaseContext.stop() // note this does nothing if it was already stopped
      
      // using Prop.Undecided allows us to return undecided if the test case is not
      // long enough (i.e. it is a word with not enough letters) to get a conclusive 
      // formula evaluation
      val testCaseResult = testCaseContext.currFormula.result.getOrElse(Prop.Undecided)  
        
      // Note: ScalaCheck will show the correct test case that caused the counterexample
      // because only the test case that generated that counterexample will fail. Anyway we could
      // use testCaseResult.mapMessage here to add testCaseDstream to the message of 
      // testCaseResult if we needed it
      logger.warn(s"finished test case $testCaseId with result $testCaseResult")
      testCaseResult match {
        case Prop.True => Prop.passed
        case Prop.Proof => Prop.proved
        case Prop.False => Prop.falsified
        case Prop.Undecided => Prop.passed //Prop.undecided FIXME make configurable
        case Prop.Exception(e) => Prop.exception(e)
      }
    }     
  }
}

class PropExecutionException(msg : String)
  extends RuntimeException(msg) 
object TestCaseTimeoutException {
  def apply(batchInterval : Long, batchCompletionTimeout : Long) : TestCaseTimeoutException = {
    val msg = s"Timeout to complete batch after ${batchCompletionTimeout} ms, expected batch interval was ${batchInterval} ms"
    new TestCaseTimeoutException(msg = msg)
  }
}
case class TestCaseTimeoutException(msg : String)
  extends PropExecutionException(msg)

object TestCaseContext {
  @transient private val logger = Logger(LoggerFactory.getLogger("TestCaseContext"))
  
   // Constants used for printing a sample of the generated values for each batch
  val msgHeader = "-"*43
  val numSampleRecords = 4
}
/** 
 *  Objects of this class define the DStreams involved in the test case execution
 *  from the test case and the provided DStream, maintain a formula object and
 *  register an action to evaluate that formula. 
 *  
 *  ssc should be fresh and not started yet
 * */
class TestCaseContext[E1:ClassTag,E2:ClassTag] 
  (@transient private val testCaseDstream : Seq[Seq[E1]], 
  @transient private val gt1 : (DStream[E1]) => DStream[E2], 
  @transient private val formulaNext: NextFormula[(RDD[E1], RDD[E2])])
  (@transient private val ssc : StreamingContext, @transient private val parallelism : Parallelism) 
  extends Serializable {
    
  import TestCaseContext.{logger,msgHeader,numSampleRecords}
  type U = (RDD[E1], RDD[E2])
  
  /* Whether the streaming context has started or not
   * */
  private var started = false
  
  // -----------------------------------
  // create input and output DStreams
  @transient val inputDStream1 = new TestInputStream[E1](
      ssc.sparkContext, ssc, testCaseDstream, parallelism.numSlices)
  inputDStream1.foreachRDD { (rdd, time) => 
      println(s"""${msgHeader}
Time: ${time} - InputDStream1 (${rdd.count} records)
${msgHeader}
${rdd.take(numSampleRecords).mkString(lineSeparator)}
...""")
  }
  @transient val transformedStream1 = gt1(inputDStream1)  
  // note we access transformedStream1 but only in slice(), so we need some trivial action
  // on transformedStream1 or we get org.apache.spark.SparkException: org.apache.spark.streaming.dstream.MappedDStream@459bd6af has not been initialized)
  transformedStream1.foreachRDD {rdd => {}}

  // -----------------------------------
  // Register actions to evaluate the formula
  // volatile as those are read both from the foreachRDD below and the Prop.forall below
  // - only foreachRDD writes to currFormula
  // - DStreamTLProperty.forAllDStream reads currFormula
  // thus using "the cheap read-write lock trick" https://www.ibm.com/developerworks/java/library/j-jtp06197/ 
  @transient @volatile var currFormula : NextFormula[U] = { 
    val currFormulaLock = new Serializable{}
    inputDStream1
    .foreachRDD { (input1Batch, time) =>
      // NOTE: batch cannot be completed until this code finishes, use
      // future if needed to avoid blocking the batch completion
      // FIXME: consider whether this synchronization is not already 
      // implicitly obtained by DStreamTLProperty.forAllDStream blocking until the batch is completed
      currFormulaLock.synchronized {
        if ((currFormula.result.isEmpty) && ! input1Batch.isEmpty) {
          /* Note currFormula is reset to formulaNext for each test case, but for
           * each test case currFormula only gets to solved state once. 
           * */
          val trans1Batch = transformedStream1.slice(time, time).head
          currFormula = currFormula.consume((input1Batch, trans1Batch))
        }
      }
    }
    // this block is just to avoid adding unnecessary fields 
    // to this object, as this is part of the constructor
    formulaNext 
  }
    
  // -----------------------------------
  // Synchronization stuff
  // won't wait for each batch for more than batchCompletionTimeout milliseconds
  @transient private val batchInterval = inputDStream1.slideDuration.milliseconds
  @transient private val batchCompletionTimeout = batchInterval * 1000 // give a good margin, values like 5 lead to spurious errors
  // the worker thread uses the SyncVar with a registered addStreamingListener
  // that notifies onBatchCompleted.
  // Note: no need to wait for receiver, as there is no receiver
  @transient private val onBatchCompletedSyncVar = new SyncVar[Unit]
  ssc.addStreamingListener(new StreamingListener {
    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) : Unit =  {
      // signal the property about the completion of a new batch  
      if (! onBatchCompletedSyncVar.isSet) {
        // note only this threads makes puts, so no problem with concurrency
        onBatchCompletedSyncVar.put(())
      }
    }
  })
  
  // -----------------------------------
  // now that everything is ready we start the streaming context
  ssc.start()
  started = true
    
  def waitForBatch() : Unit = {
    Try {
      onBatchCompletedSyncVar.take(batchCompletionTimeout)
    } match {
        case Success(_) => {}
        case Failure(_) => {
          val e = TestCaseTimeoutException(batchInterval= batchInterval, 
                                           batchCompletionTimeout = batchCompletionTimeout)
          logger.error(e.getMessage) // FIXME should be private
          Try { ssc.stop(stopSparkContext = false, stopGracefully = false) }
          // This exception will make the test case fail, in this case the 
          // failing test case is not important as this is a performance problem, not 
          // a counterexample that has been found
          throw e
        }
      }    
  }
    
  /** Stops the internal streaming context, if it is running
   *  
   *  TODO: consider moving this to DStreamTLProperty
   * */
  def stop() : Unit = {
    if (started) {
      Try { 
        logger.warn("stopping test Spark Streaming context")
        ssc.stop(stopSparkContext = false, stopGracefully = true)
        started = false
      } recover {
          case _ => {
            logger.warn("second attempt forcing stop of test Spark Streaming context")
            ssc.stop(stopSparkContext=false, stopGracefully=false)
            started = false
          }
        }
    }
  }
    
}