package es.ucm.fdi.sscheck.prop.tl

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

import org.apache.spark.streaming.dstream.{DynSingleSeqQueueInputDStream}
import es.ucm.fdi.sscheck.spark.Parallelism
import es.ucm.fdi.sscheck.{TestCaseIdCounter,PropResult,TestCaseId,TestCaseRecord}
  
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

object DStreamProp {
  @transient private val logger = Logger(LoggerFactory.getLogger("DStreamProp"))
  
  // Constants used for printing a sample of the generated values for each batch
  val msgHeader = "-"*43
  val numSampleRecords = 4
  
  val defaultTimeout = Timeout(10)
  
  /** Define a ScalaCheck property for 1 input dstream and 1 transformed dstream, 
   *  that succeeds iff all the formula holds for each generated test case
   *  
   * NOTE: currently only supports a single ScalaCheck worker
   * */
  def forAll[E1:ClassTag,E2:ClassTag,T <% Timeout]
            (g1: Gen[Seq[Seq[E1]]])(gt1 : (DStream[E1]) => DStream[E2])
            (formula : Formula[(RDD[E1], RDD[E2])], on : T = defaultTimeout)
            (implicit pp1: Seq[Seq[E1]] => Pretty, 
                      ssc : StreamingContext, parallelism : Parallelism): Prop = {
    
    type U = (RDD[E1], RDD[E2])
    type Form = Formula[U]
    // using DynSeqQueueInputDStream and addDStream leads to mixing the end of a test
    // case with the start of the next one
    //val inputDStream1 = new DynSeqQueueInputDStream[E1](ssc, numSlices = parallelism.numSlices)
    val inputDStream1 = new DynSingleSeqQueueInputDStream[E1](ssc, numSlices = parallelism.numSlices)
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
    
    val formulaNext = formula.nextFormula
    // volatile as those are read both from the foreachRDD below and the Prop.forall below
    // - only Prop.forall writes to resetFormula, to signal foreachRDD to reset the formula
    // - only foreachRDD writes to currFormula
    // - Prop.forall reads currFormula, and foreachRDD reads resetFormula
    @volatile var currFormula : NextFormula[U] = null // effectively initialized to formulaNext, if the code is ok
      // for "the cheap read-write lock trick" https://www.ibm.com/developerworks/java/library/j-jtp06197/ 
    val currFormulaLock = new Serializable{}
    @volatile var resetFormula = true
    inputDStream1
    .foreachRDD { (input1Batch, time) =>
      if (resetFormula) currFormula = formulaNext
      // NOTE: batch cannot be completed until this code finishes, use
      // future if needed to avoid blocking the batch completion
      // FIXME: consider whether this synchronization is not already 
      // implicitly obtained by Prop.forall blocking until the batch is completed
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

    // Exploit the property execution to send each test case to the DynSeqQueueInputDStream
    // the formula is evaluated by the foreachRDD action above
    Prop.forAllNoShrink (g1) { (testCaseDstream : Seq[Seq[E1]]) =>
      // Setup new test case
      val testCaseId : TestCaseId = testCaseIdCounter.nextId()
        // reset currFormula, we are starting again for the new test case
      resetFormula = true
        // we use propFailed to stop in the middle of the test case as soon as a counterexample is found 
        // Note: propFailed is not equivalent to currFormula.result.isDefined, because propFailed is
        // only defined after a wait for onBatchCompleted
      var propFailed = false 
      logger.warn(s"starting test case $testCaseId")
        // set current test case at inputDStream
      inputDStream1.setDStream(testCaseDstream)
      
      for (i <- 1 to testCaseDstream.length if (! propFailed)) {
        // await for the end of the each batch 
        logger.debug(s"waiting end of batch ${i} of test case ${testCaseId} at ${Thread.currentThread()}")
          // use the SyncVar to wait for batch completion
        Try {
          onBatchCompletedSyncVar.take(batchCompletionTimeout)
        } match {
          case Success(_) => {}
          case Failure(_) => {
            val e = TestCaseTimeoutException(batchInterval= batchInterval, 
                                             batchCompletionTimeout = batchCompletionTimeout)
            logger.error(e.getMessage)
            Try { ssc.stop(stopSparkContext = false, stopGracefully = false) }
            // This exception will make the test case fail, in this case the 
            // failing test case is not important as this is a performance problem, not 
            // a counterexample that has been found
            throw e
          }
        }
        if (i == 1) resetFormula = false // don't make foreachRDD reset the formula after the first batch
        logger.debug(s"awake after end of batch ${i} of test case ${testCaseId} at ${Thread.currentThread()}")         
        if (currFormula.result.isDefined && {
            val currFormulaResult = currFormula.result.get
            currFormulaResult == Prop.False || currFormulaResult.isInstanceOf[Prop.Exception]
            }) {  
          // some batch generated a counterexample
          propFailed = true  
          Try { ssc.stop(stopSparkContext = false, stopGracefully = false) }
        }
        // else do nothing, as the data is already sent 
      }
      // using Prop.Undecided allows us return undecided if the test case is not
      // long enough (i.e. it is a word with not enough letters) to get a conclusive 
      // formula evaluation
      val testCaseResult = currFormula.result.getOrElse(Prop.Undecided)  
        
      // Note: ScalaCheck will show the correct test case that caused the counterexample
      // because only the test case generated that counterexample will fail. Anyway we could
      // use testCaseResult.mapMessage here to add testCaseDstream to the message of 
      // testCaseResult if we needed it
      logger.warn(s"finished test case $testCaseId with result $testCaseResult")
      testCaseResult match {
        case Prop.True => Prop.passed
        case Prop.Proof => Prop.proved
        case Prop.False => Prop.falsified
        case Prop.Undecided => Prop.passed //Prop.undecided FIXME
        case Prop.Exception(e) => Prop.exception(e)
      }
    }     
  }
  
 
  
}
