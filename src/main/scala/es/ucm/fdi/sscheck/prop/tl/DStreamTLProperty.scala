package es.ucm.fdi.sscheck.prop.tl

import org.scalacheck.Gen
import org.scalacheck.Prop
import org.scalacheck.util.Pretty
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Duration}
import org.apache.spark.streaming.{Time => SparkTime}
import scala.reflect.ClassTag
import scala.concurrent.{Future, ExecutionContext}
import java.util.concurrent.Executors
import scala.util.{Try, Success, Failure}
import org.slf4j.LoggerFactory
import scala.util.Properties.lineSeparator
import scala.language.implicitConversions
import es.ucm.fdi.sscheck.gen.UtilsGen.optGenToGenOpt
import es.ucm.fdi.sscheck.{TestCaseIdCounter,TestCaseId}
import es.ucm.fdi.sscheck.spark.{SharedSparkContextBeforeAfterAll,Parallelism}
import es.ucm.fdi.sscheck.spark.streaming
import es.ucm.fdi.sscheck.spark.streaming.TestInputStream

import DStreamTLProperty.SSeq
import com.google.common.collect.Synchronized

/*
 * TODO
 * - see https://issues.apache.org/jira/browse/SPARK-12009 and see if update to Spark 2.0.0 fixes 
 * 
16/10/03 04:09:55 ERROR StreamingListenerBus: StreamingListenerBus has already stopped! Dropping event StreamingListenerBatchCompleted(BatchInfo(1475467783650 ms,Map(),1475467783676,Some(1475467795274),Some(1475467795547),Map(0 -> OutputOperationInfo(1475467783650 ms,0,foreachRDD at DStreamTLProperty.scala:258,org.apache.spark.streaming.dstream.DStream.foreachRDD(DStream.scala:668)
es.ucm.fdi.sscheck.prop.tl.TestCaseContext$.es$ucm$fdi$sscheck$prop$tl$TestCaseContext$$printDStream(DStreamTLProperty.scala:258)
es.ucm.fdi.sscheck.prop.tl.TestCaseContext.init(DStreamTLProperty.scala:334)
 * 
 * that is an error message than is thrown during the graceful stop of the streaming context, 
 * and DStreamTLProperty.scala:258 is the foreachRDD at TestCaseContext.printDStream()
 * 
 * - see https://issues.apache.org/jira/browse/SPARK-14701 and see if update to Spark 2.0.0 fixes
 * 
java.util.concurrent.RejectedExecutionException: Task org.apache.spark.streaming.CheckpointWriter$CheckpointWriteHandler@1a08d2 rejected from java.util.concurrent.ThreadPoolExecutor@b03cb4[Terminated, pool size = 0, active threads = 0, queued tasks = 0, completed tasks = 70]
	at java.util.concurrent.ThreadPoolExecutor$AbortPolicy.rejectedExecution(ThreadPoolExecutor.java:2047)
	at java.util.concurrent.ThreadPoolExecutor.reject(ThreadPoolExecutor.java:823)
	at java.util.concurrent.ThreadPoolExecutor.execute(ThreadPoolExecutor.java:1369)
	at org.apache.spark.streaming.CheckpointWriter.write(Checkpoint.scala:278)
	at org.apache.spark.streaming.scheduler.JobGenerator.doCheckpoint(JobGenerator.scala:295)
	at org.apache.spark.streaming.scheduler.JobGenerator.org$apache$spark$streaming$scheduler$JobGenerator$$processEvent(JobGenerator.scala:184)
	at org.apache.spark.streaming.scheduler.JobGenerator$$anon$1.onReceive(JobGenerator.scala:87)
	at org.apache.spark.streaming.scheduler.JobGenerator$$anon$1.onReceive(JobGenerator.scala:86)
	at org.apache.spark.util.EventLoop$$anon$1.run(EventLoop.scala:48)
16/10/03 04:34:17 ERROR StreamingListenerBus: StreamingListenerBus has already stopped! Dropping event StreamingListenerBatchCompleted(BatchInfo(1475469252300 
 * */

object DStreamTLProperty {
  @transient private val logger = LoggerFactory.getLogger("DStreamTLProperty")
  
  type SSeq[A] = Seq[Seq[A]]
  type SSGen[A] = Gen[SSeq[A]]
}

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
  
  /** Override for custom configuration
  *  Whether the input and output RDDs for each test case should be cached 
  *  (with RDD.cache) or not, true by default
  *  
  *  Set to false if you get"Uncaught exception: RDDBlockId not found in driver-heartbeater", 
  *  "java.lang.ClassNotFoundException: org.apache.spark.storage.RDDBlockId", 
  *  "ERROR TaskSchedulerImpl: Lost executor driver on localhost: Executor heartbeat timed out" or
  *  "ExecutorLostFailure (executor driver exited caused by one of the running tasks) Reason: Executor heartbeat timed out"
  *  as it is a known problem https://issues.apache.org/jira/browse/SPARK-10722, that was reported
  *  as fixed in Spark 1.6.2 in SPARK-10722  
  */
  def catchRDDs: Boolean = true
  
  /** Override for custom configuration
  *  Maximum number of micro batches that the test case will wait for, 100 by default 
  * */
  def maxNumberBatchesPerTestCase: Int = 100
  
  /** @return a newly created streaming context, for which no DStream or action has 
   *  been defined, and that it's not started
   *  
   *  Precondition: no streaming context is currently started
   *  in this JVM
   * */
  private def buildFreshStreamingContext() : StreamingContext = {
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
   
  // 1 in 1 out
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
    gen1: SSGen[I1])(
    trans1: (DStream[I1]) => DStream[O1])(
    formula: Formula[(RDD[I1], RDD[O1])])(
    implicit pp1: SSeq[I1] => Pretty): Prop = {
      
    type U = (RDD[I1], RDD[O1])
    implicit def atomsAdapter(a1: Option[RDD[I1]], a2: Option[RDD[Unit]], 
                              a3: Option[RDD[O1]], a4: Option[RDD[Unit]]): U = 
      (a1.get, a3.get)
    forAllDStreamOption[I1,Unit,O1,Unit,U](
      gen1, None)(
      (ds1, ds2) => trans1(ds1.get), 
      None)(
      formula)
  }
  
  // 1 in 2 out
  /** 1 input - 2 outputs version of [[DStreamTLProperty.forAllDStream[I1,O1]:Prop*]].
   * */
  def forAllDStream12[I1:ClassTag,O1:ClassTag,O2:ClassTag](
    gen1: SSGen[I1])(
    trans1: (DStream[I1]) => DStream[O1], 
    trans2: (DStream[I1]) => DStream[O2])(
    formula: Formula[(RDD[I1], RDD[O1], RDD[O2])])(
    implicit pp1: SSeq[I1] => Pretty): Prop = {
    
    type U = (RDD[I1], RDD[O1], RDD[O2])
    implicit def atomsAdapter(a1: Option[RDD[I1]], a2: Option[RDD[Unit]], 
                              a3: Option[RDD[O1]], a4: Option[RDD[O2]]): U = 
      (a1.get, a3.get, a4.get)
    forAllDStreamOption[I1,Unit,O1,O2,U](
      gen1, None)(
      (ds1, ds2) => trans1(ds1.get), 
      Some((ds1, ds2) => trans2(ds1.get)))(
      formula)
  }
    
  // 2 in 1 out
  /** 2 inputs - 2 output version of [[DStreamTLProperty.forAllDStream[I1,O1]:Prop*]].
   * */
  def forAllDStream21[I1:ClassTag,I2:ClassTag,O1:ClassTag](
    gen1: SSGen[I1], gen2: SSGen[I2])(
    trans1: (DStream[I1], DStream[I2]) => DStream[O1])(
    formula: Formula[(RDD[I1], RDD[I2], RDD[O1])])(
    implicit pp1: SSeq[I1] => Pretty, pp2: SSeq[I2] => Pretty): Prop = {
    
    type U = (RDD[I1], RDD[I2], RDD[O1])
    implicit def atomsAdapter(a1: Option[RDD[I1]], a2: Option[RDD[I2]], 
                              a3: Option[RDD[O1]], a4: Option[RDD[Unit]]): U = 
      (a1.get, a2.get, a3.get)
    forAllDStreamOption[I1,I2,O1,Unit,U](
      gen1, Some(gen2))(
      (ds1, ds2) => trans1(ds1.get, ds2.get), 
      None)(
      formula)
  }

  // 2 in 2 out
  /** 2 inputs - 2 outputs version of [[DStreamTLProperty.forAllDStream[I1,O1]:Prop*]].
   * */
  def forAllDStream22[I1:ClassTag,I2:ClassTag,O1:ClassTag,O2:ClassTag](
    gen1: SSGen[I1], gen2: SSGen[I2])(
    trans1: (DStream[I1], DStream[I2]) => DStream[O1],
    trans2: (DStream[I1], DStream[I2]) => DStream[O2])(
    formula: Formula[(RDD[I1], RDD[I2], RDD[O1], RDD[O2])])(
    implicit pp1: SSeq[I1] => Pretty, pp2: SSeq[I2] => Pretty): Prop = {
    
    type U = (RDD[I1], RDD[I2], RDD[O1], RDD[O2])
    implicit def atomsAdapter(a1: Option[RDD[I1]], a2: Option[RDD[I2]], 
                              a3: Option[RDD[O1]], a4: Option[RDD[O2]]): U = 
      (a1.get, a2.get, a3.get, a4.get)
    forAllDStreamOption[I1,I2,O1,O2,U](
      gen1, Some(gen2))(
      (ds1, ds2) => trans1(ds1.get, ds2.get), 
      Some((ds1, ds2) => trans2(ds1.get, ds2.get)))(
      formula)
  }
  
  // TODO: add scaladoc
  private def forAllDStreamOption[I1:ClassTag,I2:ClassTag,
                             O1:ClassTag,O2:ClassTag,
                             U](
    g1: SSGen[I1], g2: Option[SSGen[I2]])(
    gt1: (Option[DStream[I1]], Option[DStream[I2]]) => DStream[O1],
    gtOpt2: Option[(Option[DStream[I1]], Option[DStream[I2]]) => DStream[O2]])(
    formula: Formula[U])(
    implicit pp1: SSeq[I1] => Pretty, pp2: SSeq[I2] => Pretty, 
     atomsAdapter: (Option[RDD[I1]], Option[RDD[I2]], Option[RDD[O1]], Option[RDD[O2]]) => U
    ): Prop = {
      
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
        new TestCaseContext[I1,I2,O1,O2,U](testCase1, testCaseOpt2, 
                                           gt1, gtOpt2,
                                           formulaNext, atomsAdapter)(
                                           freshSsc, parallelism, catchRDDs, maxNumberBatchesPerTestCase)
      logger.warn(s"starting test case $testCaseId")
      testCaseContext.init()
      /*
       * We have to ensure we block until the StreamingContext is stopped before creating a new StreamingContext 
       * for the next test case, otherwise we get "java.lang.IllegalStateException: Only one StreamingContext may 
       * be started in this JVM" (see SPARK-2243)  
       * */
      testCaseContext.waitForCompletion()
      // In case there was a timeout waiting for test execution, again to avoid more than one StreamingContext  
      // note this does nothing if it was already stopped
      testCaseContext.stop() 
     
      val testCaseResult = testCaseContext.result
      
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
  def apply(batchInterval: Long, batchCompletionTimeout: Long): TestCaseTimeoutException = {
    val msg = s"Timeout to complete batch after ${batchCompletionTimeout} ms, expected batch interval was ${batchInterval} ms"
    new TestCaseTimeoutException(msg = msg)
  }
}
case class TestCaseTimeoutException(msg: String)
  extends PropExecutionException(msg)

object TestCaseContext {
  @transient private val logger = LoggerFactory.getLogger("TestCaseContext")
  
  // Constants used for printing a sample of the generated values for each batch
  private val msgHeader = "-"*43
  private val numSampleRecords = 4
  
  /* A single execution context for all test cases because we are limited to 
   * one test case per JVM due to the 1 streaming context per JVM limitation 
   * of SPARK-2243
   */
  implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1))

  /** Print some elements of dstream to stdout
   */
  private def printDStream[A](dstream: DStream[A], dstreamName: String): Unit = 
    dstream.foreachRDD { (rdd, time) => 
      if (!rdd.isEmpty()) {
        val numRecords = rdd.count
        println(s"""${msgHeader}
Time: ${time} - ${dstreamName} (${numRecords} records)
${msgHeader}""")
        if (numRecords > 0) println(rdd.take(numSampleRecords).mkString(lineSeparator))
        println("...")
      }
    }
  /** Launch a trivial action on dstream to force its computation
   */
  private def touchDStream[A](dstream: DStream[A]): Unit = 
    dstream.foreachRDD {rdd => {}}
    
  /* Note https://issues.apache.org/jira/browse/SPARK-10722 causes 
   * "Uncaught exception: RDDBlockId not found in driver-heartbeater", 
   * "java.lang.ClassNotFoundException: org.apache.spark.storage.RDDBlockId", 
   * "ERROR TaskSchedulerImpl: Lost executor driver on localhost: Executor heartbeat timed out" and
   * "ExecutorLostFailure (executor driver exited caused by one of the running tasks) Reason: Executor heartbeat timed out"
   * but that reported as fixed in v 1.6.2 in SPARK-10722  
   */
  private def getBatchForNow[T](ds: DStream[T], time: SparkTime, catchRDD: Boolean): RDD[T] = {
    val batch = ds.slice(time, time).head
    if (catchRDD && batch.getStorageLevel == StorageLevel.NONE) batch.cache else batch
  } 
}
/** 
 *  Objects of this class define the DStreams involved in the test case execution
 *  from the test case and the provided DStream, maintain a formula object and
 *  register an action to evaluate that formula. 
 *  
 *  ssc should be fresh and not started yet
 */
class TestCaseContext[I1:ClassTag,I2:ClassTag,O1:ClassTag,O2:ClassTag, U](
  @transient private val testCase1: SSeq[I1], 
  @transient private val testCaseOpt2: Option[SSeq[I2]],
  gt1: (Option[DStream[I1]], Option[DStream[I2]]) => DStream[O1],
  gtOpt2: Option[(Option[DStream[I1]], Option[DStream[I2]]) => DStream[O2]],
  @transient private val formulaNext: NextFormula[U], 
  @transient private val atomsAdapter: (Option[RDD[I1]], Option[RDD[I2]], Option[RDD[O1]], Option[RDD[O2]]) => U)
  (@transient private val ssc : StreamingContext, 
   @transient private val parallelism : Parallelism, @transient private val catchRDDs: Boolean, 
   @transient private val maxNumberBatches: Int) 
  extends Serializable {
  /*
   *  With the constructor types we enforce having at least a non empty test case and a 
   *  non empty transformation. That assumption simplifies the code are 
   *  it is not limiting, because you need at least one input and one transformation
   *  in order to be testing something!
   */
  // TODO add assertions on Option compatibilities
  
  import TestCaseContext.{logger,printDStream,touchDStream,ec,getBatchForNow}
  
  /** Current value of the formula we are evaluating in this test case contexts 
   */
  @transient @volatile private var currFormula: NextFormula[U] = formulaNext 
  // Number of batches in the test case that are waiting to be executed
  @transient @volatile private var numRemaningBatches: Int = 
    (testCase1.length) max (testCaseOpt2.fold(0){_.length})
  
  /** Whether the streaming context has started or not
   * */
  private var started = false
  
  // have to create this here instead of in init() because otherwise I don't know how
  // to access the batch interval of the streaming context
  @transient private val inputDStream1 = 
      new TestInputStream[I1](ssc.sparkContext, ssc, testCase1, parallelism.numSlices)
  // batch interval of the streaming context
  @transient private val batchInterval = inputDStream1.slideDuration.milliseconds

  def init(): Unit = this.synchronized {
    // -----------------------------------
    // create input and output DStreams
    @transient val inputDStreamOpt1 = Some(inputDStream1)
    @transient val inputDStreamOpt2 = testCaseOpt2.map{
      new TestInputStream[I2](ssc.sparkContext, ssc, _, parallelism.numSlices)}
    printDStream(inputDStream1, "InputDStream1")
    inputDStreamOpt2.foreach{printDStream(_, "InputDStream2")}

    // note we although we do access these transformed DStream, we only access them in slice(), 
    // so we need some trivial action on transformedStream1 or we get org.apache.spark.SparkException: 
    // org.apache.spark.streaming.dstream.MappedDStream@459bd6af has not been initialized)
    @transient val transformedStream1 = gt1(inputDStreamOpt1, inputDStreamOpt2)
    @transient val transformedStreamOpt2 = gtOpt2.map{_.apply(inputDStreamOpt1, inputDStreamOpt2)}
    touchDStream(transformedStream1)
    transformedStreamOpt2.foreach{touchDStream _}
  
    // -----------------------------------
    // Register actions to evaluate the formula
    // volatile as those are read both from the foreachRDD below and the Prop.forall below
    // - only foreachRDD writes to currFormula
    // - DStreamTLProperty.forAllDStreamOption reads currFormula
    // thus using "the cheap read-write lock trick" https://www.ibm.com/developerworks/java/library/j-jtp06197/
    val testCaseStateLock = new Serializable{}
    inputDStream1.foreachRDD { (inputBatch1, time) =>
      // NOTE: batch cannot be completed until this code finishes, use
      // future if needed to avoid blocking the batch completion
      testCaseStateLock.synchronized {
        if ((currFormula.result.isEmpty) && (numRemaningBatches > 0)) {
          /* Note a new TestCaseContext with its own currFormula initialized to formulaNext 
           * is created for each test case, but for each TestCaseContext currFormula 
           * only gets to solved state once. 
       	   * */
          Try {
            val inputBatchOpt1 = Some(if (catchRDDs) inputBatch1.cache else inputBatch1)
            val inputBatchOpt2 = inputDStreamOpt2.map(getBatchForNow(_, time, catchRDDs))
            val outputBatchOpt1 = Some(getBatchForNow(transformedStream1, time, catchRDDs)) 
            val outputBatchOpt2 = transformedStreamOpt2.map(getBatchForNow(_, time, catchRDDs))
            val adaptedAtoms = atomsAdapter(inputBatchOpt1, inputBatchOpt2, outputBatchOpt1, outputBatchOpt2)              
            currFormula = currFormula.consume(Time(time.milliseconds))(adaptedAtoms)
            numRemaningBatches = numRemaningBatches - 1
          } recover { case throwable =>
            // if there was some problem solving the assertions 
            // then we solve the formula with an exception 
            currFormula = Solved(Prop.Exception(throwable))
          }
          if (currFormula.result.isDefined || numRemaningBatches <= 0) { 
            Future { this.stop() } 
          }
        }
      }
    }
    
    // -----------------------------------
    // now that everything is ready we start the streaming context
    ssc.start()
    started = true
  }

  /** Blocks the caller until this test case context execution completes.
   *  The test case stops itself as soon as the formula is resolved, 
   *  in order to fail fast in the first micro-batch that finds a counterexample
   *   the next batch for the DStreams associated 
   *  to this object is completed. This way we can fail faster if a single
   *  batch is too slow, instead of waiting for a long time for the whole
   *  streaming context with ssc.awaitTerminationOrTimeout()
   *  
   *  @return true iff the formula for this test case context is resolved, 
   *  otherwise return false
   */
  def waitForCompletion(): Unit = {
    this.ssc.awaitTerminationOrTimeout(batchInterval * (maxNumberBatches*1.3).ceil.toInt)
  }
  
  /** @return The result of the execution of this test case, or Prop.Undecided
   *  if the result is inconclusive (e.g. because the test case it's not
   *  long enough), or yet unknown. 
   * */
  def result: Prop.Status = currFormula.result.getOrElse(Prop.Undecided)
  
  /** Stops the internal streaming context, if it is running  
   */
  def stop() : Unit = this.synchronized {
    if (started) {
      Try { 
        logger.warn("stopping test Spark Streaming context") 
        ssc.stop(stopSparkContext = false, stopGracefully = true)
        Thread.sleep(1000L)
        ssc.stop(stopSparkContext = false, stopGracefully = false)
      } recover {
          case _ => {
            logger.warn("second attempt forcing stop of test Spark Streaming context")
            ssc.stop(stopSparkContext = false, stopGracefully = false)
          }
      }
      started = false
    }
  }
}
