package es.ucm.fdi.sscheck.spark.streaming

import org.specs2.specification.BeforeAfterEach

import org.apache.spark.streaming.StreamingContext
  
import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll

/** Shares a Spark Context among all the test in a Specs2 suite, and provides a StreamingContext 
 *  for each example a suite. Each example has its own StreamingContext because no transformations
 *  nor actions can be added to a StreamingContext after it has started (see  
 *  [[https://spark.apache.org/docs/latest/streaming-programming-guide.html#initializing-streamingcontext points to remember]]).
 *  On the other hand, a StreamingContext can be shared among all the test cases of a ScalaCheck property,
 *  as the same property (hence the same Spark transformations and actions) is applied to all the test cases.
 *  
 *  Life cycle: a SparkContext is created beforeAll and closed afterAll. A StreamingContext 
 *  is created before each Specs2 example and closed after each example
 *  
 *  WARNING: when using in Specs2 specifications, [[https://etorreborre.github.io/specs2/guide/SPECS2-3.6.2/org.specs2.guide.Execution.html sequential execution]] 
 *  must be enabled to avoid having more than a single StreamingContext started in the same JVM 
 *    
 *  Note the shared StreamingContext is not made implicitly available, as it happens
 *  with SparkContext in SharedSparkContextBeforeAfterAll. This is due to the more
 *  complex life cycle of streaming contexts, and the fact that they doesn't accept
 *  registering more computations after they have started 
 */
trait SharedStreamingContextBeforeAfterEach
  extends BeforeAfterEach
  with SharedSparkContextBeforeAfterAll
  with SharedStreamingContext {
  
  /** Force the creation of the StreamingContext before the test
   * */
  override def before : Unit = {
    require(_ssc.isEmpty)
    this.ssc()
  }
  
  /** Close the StreamingContext after the test
   */
  override def after : Unit = {
    // We don't require ! _ssc.isEmpty, as it 
    // might have been already closed by a failing ScalaCheck Prop
    super[SharedStreamingContext].close()
  }
  
  /** Make implicitly available the StreamingContext 
   * */                      		  
  implicit def impSSC : StreamingContext = this.ssc()
}