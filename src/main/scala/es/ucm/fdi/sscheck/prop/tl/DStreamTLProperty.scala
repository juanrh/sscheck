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

import org.slf4j.LoggerFactory

import scala.util.Properties.lineSeparator
import scala.language.implicitConversions

import es.ucm.fdi.sscheck.{TestCaseIdCounter,TestCaseId}
import es.ucm.fdi.sscheck.spark.{SharedSparkContextBeforeAfterAll,Parallelism}
import es.ucm.fdi.sscheck.spark.streaming
import es.ucm.fdi.sscheck.spark.streaming.TestInputStream

object DStreamTLProperty {
  @transient private val logger = LoggerFactory.getLogger("DStreamTLProperty")
  
  type SSeq[A] = Seq[Seq[A]]
  type SSGen[A] = Gen[SSeq[A]]
}

import DStreamTLProperty.SSeq

trait DStreamTLProperty 
  extends SharedSparkContextBeforeAfterAll {
  
  import DStreamTLProperty.{logger,SSeq,SSGen}
  
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
  
  // TODO: revise access level for all methods in this class
  // TODO: less options
      //    gt1: Option[(DStream[I1], DStream[I2]) => DStream[O1]],
//    gt2: Option[(DStream[I1], DStream[I2]) => DStream[O2]])(
  
  //    def genOptToDStreamOp[I:ClassTag](g: Option[SSGen[I]]): Option[DStream[I]] = 
//      g.map { gen =>
//        val batches = List(gen.sample.get, gen.sample.get)
//        new TestInputStream(???, ???, gen.sample.get, 2)
//      }
//    val (dsIn1, dsIn2) = (genOptToDStreamOp(g1), genOptToDStreamOp(g2))
//    /* HERE FIXME: cannot use gt1 so that it fails when g1 or g2 leads to dsIn1 or dsIn2 
//     * being none, as for example forAllDStream11Option ignores the second argument. 
//     * An easy option is making the DStreams also an Option, i.e.  
//     * gt1: Option[(Option[DStream[I1]], Option[DStream[I2]]) => DStream[O1]]
//     * 
//    */
//    //val gt1fix: Option[(Option[DStream[I1]], Option[DStream[I2]]) => DStream[O1]] = ???
//    val dsOut2 = gt1.map(_.apply(dsIn1, dsIn2)) //gt1fix.map(_.apply(dsIn1, dsIn2))
    
//    // probably HList here could be useful to get the not absent dsi, for a generalization to more dstreams
//    dsIn1.get.foreachRDD { (inBatch1, time) =>
//      val inBatch2: RDD[I2] = ??? // with slice, as there is no foreachRDDWith
//    }
    
//    type UWrapper = (Option[RDD[I1]], Option[RDD[I2]], Option[RDD[O1]], Option[RDD[O2]])
//    val ats: UWrapper = ???
//    val atsAdapted = atomsAdapter.tupled(ats) 
//    formula.nextFormula.consume(???)(atsAdapted)
    /*
Multiple markers at this line:
◾type mismatch; found : ats.type (with underlying type UWrapper) required: U
◾type mismatch; found : ats.type (with underlying type (Option[org.apache.spark.rdd.RDD[I1]], Option[org.apache.spark.rdd.RDD[Unit]], Option[org.apache.spark.rdd.RDD[O1]], Option[org.apache.spark.rdd.RDD[Unit]])) required: U

     * */

  /*
class TestCaseContext22[I1:ClassTag,I2:ClassTag,O1:ClassTag,O2:ClassTag, U](
  @transient private val testCase1: SSeq[I1], 
  @transient private val testCaseOpt2: Option[SSeq[I2]],
  gt1: (Option[DStream[I1]], Option[DStream[I2]]) => DStream[O1],
  gtOpt2: Option[(Option[DStream[I1]], Option[DStream[I2]]) => DStream[O2]],
  @transient private val formulaNext: NextFormula[U], 
  @transient private val atomsAdapter: (Option[RDD[I1]], Option[RDD[I2]], Option[RDD[O1]], Option[RDD[O2]]) => U)
  (@transient private val ssc : StreamingContext, @transient private val parallelism : Parallelism)    
   */
  
  private def forAllDStream22Option[I1:ClassTag,I2:ClassTag,
                             O1:ClassTag,O2:ClassTag,
                             U](
    g1: SSGen[I1], g2: Option[SSGen[I2]])(
    gt1: (Option[DStream[I1]], Option[DStream[I2]]) => DStream[O1],
    gtOpt2: Option[(Option[DStream[I1]], Option[DStream[I2]]) => DStream[O2]])(
    //formula: Formula[(RDD[I1], Option[RDD[I2]], RDD[O1], Option[RDD[O2]])])(
    formula: Formula[U])(
    implicit pp1: SSeq[I1] => Pretty, pp2: SSeq[I2] => Pretty, 
     atomsAdapter: (Option[RDD[I1]], Option[RDD[I2]], Option[RDD[O1]], Option[RDD[O2]]) => U
    ): Prop = {
     
    // TODO move a Gen utils
    def optGenToGenOpt[A](gen: Option[Gen[A]]): Gen[Option[A]] = 
      gen.map(_.map(Some(_))).getOrElse(Gen.const(None))
      
    val formulaNext = formula.nextFormula
    // test case id counter / generator
    val testCaseIdCounter = new TestCaseIdCounter
    
    // Create a new streaming context per test case, and use it to create a new TestCaseContext
    // that will use TestInputStream from spark-testing-base to create new input and output 
    // dstreams, and register a foreachRDD action to evaluate the formula
    Prop.forAllNoShrink (g1, optGenToGenOpt(g2)) { (testCase1: SSeq[I1], testCaseOpt2: Option[SSeq[I2]]) =>
      // Setup new test case
      val testCaseId : TestCaseId = testCaseIdCounter.nextId() 
      // create, start and stop context for each test case      
        // create a fresh streaming context for this test case, and pass it unstarted to 
        // a new TestCaseContext, which will setup the streams and actions, and start the streaming context
      val freshSsc = buildFreshStreamingContext() 
      val testCaseContext = 
        new TestCaseContext22[I1,I2,O1,O2,U](testCase1, testCaseOpt2, gt1,gtOpt2,formulaNext, atomsAdapter)(freshSsc, parallelism)
        // we use propFailed to stop in the middle of the test case as soon as a counterexample is found 
        // Note: propFailed is not equivalent to currFormula.result.isDefined, because propFailed is
        // only defined after a wait for onBatchCompleted
      var propFailed = false 
      logger.warn(s"starting test case $testCaseId")      
      val maxTestCaseLength = List(testCase1.length, testCaseOpt2.map{_.length}.getOrElse(0)).max
      for (i <- 1 to maxTestCaseLength if (! propFailed)) {
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
   
  /*
 
 private def forAllDStream22Option[I1:ClassTag,I2:ClassTag,
                             O1:ClassTag,O2:ClassTag,
                             U](
    g1: Option[SSGen[I1]], g2: Option[SSGen[I2]])(
    gt1: Option[(Option[DStream[I1]], Option[DStream[I2]]) => DStream[O1]],
    gt2: Option[(Option[DStream[I1]], Option[DStream[I2]]) => DStream[O2]])(
    //formula: Formula[(RDD[I1], Option[RDD[I2]], RDD[O1], Option[RDD[O2]])])(
    formula: Formula[U])(
    implicit pp1: SSeq[I1] => Pretty, pp2: SSeq[I2] => Pretty, 
     atomsAdapter: (Option[RDD[I1]], Option[RDD[I2]], Option[RDD[O1]], Option[RDD[O2]]) => U
    )
   */
  
 
  
  def forAllDStream11Option[I1:ClassTag,O1:ClassTag](
    g1: SSGen[I1])(
    gt1: (DStream[I1]) => DStream[O1])(
    formula: Formula[(RDD[I1], RDD[O1])])(
    implicit pp1: SSeq[I1] => Pretty): Prop = {
   
      /*
       * How can I obtain a formula for (RDD[I1], Option[RDD[Unit]], RDD[O1], Option[RDD[Unit]])
       * from a formula for (RDD[I1], RDD[O1])? What I really need is a formula that has None
       * is those options. Note None is not a type but an object
       * */
    //implicit def optiontuple
    
    //val f: Formula[(RDD[I1], Option[RDD[Unit]], RDD[O1], Option[RDD[Unit]])] = ???
    type U = (RDD[I1], RDD[O1])
    type UWrapper = (Option[RDD[I1]], Option[RDD[Unit]], Option[RDD[O1]], Option[RDD[Unit]])
    // U <% (Option[RDD[I1]], Option[RDD[I2]], Option[RDD[O1]], Option[RDD[O2]])(
//    implicit def uToOptionTup4(atoms: U): UWrapper = { 
//        val (inBatch1, outBatch1) = atoms
//        (Some(inBatch1), None, Some(outBatch1), None)
//     }
    implicit def atomsAdapter(a1: Option[RDD[I1]], a2: Option[RDD[Unit]], a3: Option[RDD[O1]], a4: Option[RDD[Unit]]) : U = 
      (a1.get, a3.get)
    forAllDStream22Option[I1,Unit,O1,Unit, U](
      g1, None)(
      (ds1, ds2) => gt1(ds1.get), 
      None)(
      formula)
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
  def forAllDStream[I1:ClassTag,O1:ClassTag](
    g1: SSGen[I1])(
    gt1: (DStream[I1]) => DStream[O1])(
    formula: Formula[(RDD[I1], RDD[O1])])(
    implicit pp1: SSeq[I1] => Pretty): Prop = {
    
    val formulaNext = formula.nextFormula
    // test case id counter / generator
    val testCaseIdCounter = new TestCaseIdCounter

    // Create a new streaming context per test case, and use it to create a new TestCaseContext
    // that will use TestInputStream from spark-testing-base to create new input and output 
    // dstreams, and register a foreachRDD action to evaluate the formula
    Prop.forAllNoShrink (g1) { (testCaseDstream: SSeq[I1]) =>
      // Setup new test case
      val testCaseId : TestCaseId = testCaseIdCounter.nextId() 
      // create, start and stop context for each test case      
        // create a fresh streaming context for this test case, and pass it unstarted to 
        // a new TestCaseContext, which will setup the streams and actions, and start the streaming context
      val freshSsc = buildFreshStreamingContext() 
      val testCaseContext = new TestCaseContext[I1,O1](testCaseDstream, gt1, formulaNext)(freshSsc, parallelism)
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

object TestCaseContext22 {
  @transient private val logger = LoggerFactory.getLogger("TestCaseContext")
  
  // Constants used for printing a sample of the generated values for each batch
  private val msgHeader = "-"*43
  private val numSampleRecords = 4

  /** Print some elements of dstream to stdout
   */
  private def printDStream[A](dstream: DStream[A], dstreamName: String): Unit =  
    dstream.foreachRDD { (rdd, time) => 
    println(s"""${msgHeader}
Time: ${time} - ${dstreamName} (${rdd.count} records)
${msgHeader}
${rdd.take(numSampleRecords).mkString(lineSeparator)}
...""")
  }
  
  /** Launch a trivial action on dstream to force its computation
   */
  private def touchDStream[A](dstream: DStream[A]): Unit = 
    dstream.foreachRDD {rdd => {}}
}
  
/** With types we enforce having at least a non empty test case and a 
*  non empty transformation. That assumption simplifies the code are 
*  it is not limiting, because you need at least one input and one transformation
*  in order to be testing something!
*/
class TestCaseContext22[I1:ClassTag,I2:ClassTag,O1:ClassTag,O2:ClassTag, U](
  @transient private val testCase1: SSeq[I1], 
  @transient private val testCaseOpt2: Option[SSeq[I2]],
  gt1: (Option[DStream[I1]], Option[DStream[I2]]) => DStream[O1],
  gtOpt2: Option[(Option[DStream[I1]], Option[DStream[I2]]) => DStream[O2]],
  @transient private val formulaNext: NextFormula[U], 
  @transient private val atomsAdapter: (Option[RDD[I1]], Option[RDD[I2]], Option[RDD[O1]], Option[RDD[O2]]) => U)
  (@transient private val ssc : StreamingContext, @transient private val parallelism : Parallelism) 
  extends Serializable {
  
  // TODO add assertions on Option compatibilitys
  
  import TestCaseContext.{logger,msgHeader,numSampleRecords}
  
  /** Current value of the formula we are evaluating in this test case contexts 
   */
  @transient @volatile var currFormula: NextFormula[U] = formulaNext 
  
  /** Whether the streaming context has started or not
   * */
  private var started = false
  
  /* Synchronization stuff to wait for batch completion 
   */
  // have to create this here instead of in init() because otherwise I don't know how
  // to access the batch interval of the streaming context
  @transient private val inputDStream1 = 
      new TestInputStream[I1](ssc.sparkContext, ssc, testCase1, parallelism.numSlices)
  // won't wait for each batch for more than batchCompletionTimeout milliseconds
  @transient private val batchInterval = inputDStream1.slideDuration.milliseconds
  @transient private val batchCompletionTimeout = batchInterval * 1000 // give a good margin, values like 5 lead to spurious errors
  // the worker thread uses this SyncVar with a registered addStreamingListener
  // that notifies onBatchCompleted.
  // Note: no need to wait for receiver, as there is no receiver
  @transient private val onBatchCompletedSyncVar = new SyncVar[Unit]
  
  init()
    
  def init(): Unit = {
    // -----------------------------------
    // create input and output DStreams
    @transient val inputDStreamOpt1 = Some(inputDStream1)
    @transient val inputDStreamOpt2 = testCaseOpt2.map{
      new TestInputStream[I2](ssc.sparkContext, ssc, _, parallelism.numSlices)}
    TestCaseContext22.printDStream(inputDStream1, "InputDStream1")
    inputDStreamOpt2.foreach{TestCaseContext22.printDStream(_, "InputDStream2")}

    // note we although we do access these transformed DStream, we only access them in slice(), 
    // so we need some trivial action on transformedStream1 or we get org.apache.spark.SparkException: 
    // org.apache.spark.streaming.dstream.MappedDStream@459bd6af has not been initialized)
    @transient val transformedStream1 = gt1(inputDStreamOpt1, inputDStreamOpt2)
    @transient val transformedStreamOpt2 = gtOpt2.map{_.apply(inputDStreamOpt1, inputDStreamOpt2)}
    TestCaseContext22.touchDStream(transformedStream1)
    transformedStreamOpt2.foreach{TestCaseContext22.touchDStream _}
  
    // -----------------------------------
    // Register actions to evaluate the formula
    // volatile as those are read both from the foreachRDD below and the Prop.forall below
    // - only foreachRDD writes to currFormula
    // - DStreamTLProperty.forAllDStream reads currFormula
    // thus using "the cheap read-write lock trick" https://www.ibm.com/developerworks/java/library/j-jtp06197/
    val currFormulaLock = new Serializable{}
    inputDStream1.foreachRDD { (inputBatch1, time) =>
      // NOTE: batch cannot be completed until this code finishes, use
      // future if needed to avoid blocking the batch completion
      // FIXME: consider whether this synchronization is not already 
      // implicitly obtained by DStreamTLProperty.forAllDStream blocking until the batch is completed
      currFormulaLock.synchronized {
        if ((currFormula.result.isEmpty) && ! inputBatch1.isEmpty) {
          /* Note currFormula is reset to formulaNext for each test case, but for
       	  * each test case currFormula only gets to solved state once. 
       	  * */
          val inputBatchOpt1 = Some(inputBatch1)
          val inputBatchOpt2 = inputDStreamOpt2.map(_.slice(time, time).head)
          val outputBatchOpt1 = Some(transformedStream1.slice(time, time).head)
          val outputBatchOpt2 = transformedStreamOpt2.map(_.slice(time, time).head)
          val adaptedAtoms = atomsAdapter(inputBatchOpt1, inputBatchOpt2, outputBatchOpt1, outputBatchOpt2)
          currFormula = currFormula.consume(Time(time.milliseconds))(adaptedAtoms)
        }
      }
    }
  
    
    // -----------------------------------
    // Synchronization stuff
    // won't wait for each batch for more than batchCompletionTimeout milliseconds
    ssc.addStreamingListener(new StreamingListener {
      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit =  {
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
  }

  /** Blocks the caller until the next batch for the DStreams associated 
   *  to this object is completed
   */
  def waitForBatch(): Unit = {
    Try {
      onBatchCompletedSyncVar.take(batchCompletionTimeout)
    } match {
        case Success(_) => {}
        case Failure(_) => {
          val tcte = TestCaseTimeoutException(batchInterval= batchInterval, 
                                           batchCompletionTimeout = batchCompletionTimeout)
         // FIXME logger.error(tcte.getMessage) 
          Try { ssc.stop(stopSparkContext = false, stopGracefully = false) }
          // This exception will make the test case fail, in this case the 
          // failing test case is not important as this is a performance problem, not 
          // a counterexample that has been found
          throw tcte
        }
      }    
  }
    
  /** Stops the internal streaming context, if it is running  
   */
  def stop() : Unit = 
    if (started) {
      Try { 
        // FIXME logger.warn("stopping test Spark Streaming context")
        ssc.stop(stopSparkContext = false, stopGracefully = true)
      } recover {
          case _ => {
           // FIXME logger.warn("second attempt forcing stop of test Spark Streaming context")
            ssc.stop(stopSparkContext = false, stopGracefully = false)
          }
      }
      started = false
    }
    
}

object TestCaseContext {
  @transient private val logger = LoggerFactory.getLogger("TestCaseContext")
  
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
  val currFormulaLock = new Serializable{}
  @transient @volatile var currFormula : NextFormula[U] = { 
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
          currFormula = currFormula.consume(Time(time.milliseconds))((input1Batch, trans1Batch))
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
          val tcte = TestCaseTimeoutException(batchInterval= batchInterval, 
                                           batchCompletionTimeout = batchCompletionTimeout)
          logger.error(tcte.getMessage) 
          Try { ssc.stop(stopSparkContext = false, stopGracefully = false) }
          // This exception will make the test case fail, in this case the 
          // failing test case is not important as this is a performance problem, not 
          // a counterexample that has been found
          throw tcte
        }
      }    
  }
    
  /** Stops the internal streaming context, if it is running
   *  
   *  TODO: consider moving this to DStreamTLProperty
   * */
  def stop() : Unit = 
    if (started) {
      Try { 
        logger.warn("stopping test Spark Streaming context")
        ssc.stop(stopSparkContext = false, stopGracefully = true)
      } recover {
          case _ => {
            logger.warn("second attempt forcing stop of test Spark Streaming context")
            ssc.stop(stopSparkContext = false, stopGracefully = false)
          }
      }
      started = false
    }
    
}


/*
 FIXME delete
 
   def forAllDStream22[I1:ClassTag,I2:ClassTag,O1:ClassTag,O2:ClassTag](
    g1: SSGen[I1], g2: SSGen[I2])(
    gt1: (DStream[I1], DStream[I2]) => DStream[O1],
    gt2: (DStream[I1], DStream[I2]) => DStream[O2])(
    formula: Formula[(RDD[I1], RDD[I2], RDD[O1], RDD[O2])])(
    implicit pp1: SSeq[I1] => Pretty, pp2: SSeq[I2] => Pretty): Prop = {
    
    
    ???
  } 
  
  def forAllDStream11[I1:ClassTag,O1:ClassTag](
    g1: SSGen[I1])(
    gt1: (DStream[I1]) => DStream[O1])(
    formula: Formula[(RDD[I1], RDD[O1])])(
    implicit pp1: SSeq[I1] => Pretty): Prop = {
    
      val g2: SSGen[Unit] = Gen.const(List(List(Unit)))
      def gt2(ds1: DStream[I1], ds2: DStream[Unit]): DStream[Unit] = { 
        // could generate a TestInputStream here that always returns a single Unit
        // but that looks like a waste of resources, and will generate ugly output
        
        // TODO: try with forAllDStream22Option that takes options, and then see
        // how to define forAllDStream11 with that. At least we know pretty it's
        // not a problem now
        
        ???
      }
      
      val f: Formula[(RDD[I1], RDD[Unit], RDD[O1], RDD[Unit])] = ???
      //implicit val pp2: SSeq[Unit] => Pretty = ???
      //import scala.reflect._
      // implicit val ct = classTag[Unit]
      
      forAllDStream22[I1,Unit,O1,Unit](
          g1, g2)(
          (ds1, ds2) => gt1(ds1), 
          gt2)(
          f)
          //(
          //pp1, pp2)
           
          ???
  }
      
      
       // FIXME: should be done with first one that it is not empty 
    //   ==> easiest solution!!! Change type of testCase1 to SSeq[I1] instead of 
    // option. We don't lose expressivity because anyway at least some input and some
    // transformation is needed. So gt1 would be gt1: (Option[DStream[I1]], Option[DStream[I2]]) => DStream[O1],
    // only make rest of the arguments optional. Adapt forALLDStream22Option accordingly, only arguments after
    // the first are optional!
 
 * 
 */