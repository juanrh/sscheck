package es.ucm.fdi.sscheck.spark

import org.specs2.specification.BeforeAfterAll

import org.scalacheck.Gen
import org.scalacheck.util.Buildable

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import es.ucm.fdi.sscheck.gen.RDDGen

/** Shares a Spark Context among all the test in a Specs2 suite, this trait is also eases the use
 *  of Scalacheck with Spark, as if some tests are ScalaCheck properties, then the Spark context 
 *  is shared among all the generated test cases.
 *  
 *  The Spark context can be made explicitly available to all the tests, passing this.sc(), 
 *  or implicitly via an implicit val 
 *  
 *  Also implicits are provided to convert ScalaCheck generators of Seq into generators of RDD, 
 *  and Seq into RDD
 * */
trait SharedSparkContextBeforeAfterAll extends BeforeAfterAll
                         		       with SharedSparkContext {
  /** Force the creation of the Spark Context before any test 
   */
  override def beforeAll : Unit = { this.sc() } 
  /** Close Spark Context after all tests
   */
  override def afterAll : Unit = { this.close() }

  /* Another option would be replacing BeforeAfterAll with an eager creation
    * of this.sc through making impSC not lazy, and cleaning up in AfterAll, 
    * but I think the currnent solution has a more clear intent 
   * */   
  /** Make implicitly available the SparkContext 
   * */                      		  
  @transient implicit lazy val impSC : SparkContext = this.sc()
   
  /** Number of partitions used to parallelize Seq values into RDDs, 
   *  override in subclass for custom parallelism
   */
  def defaultParallelism: Int = 2
  /** Make implicitly available the value of parallelism finally set for this object
   * */
  @transient implicit lazy val parallelism = Parallelism(defaultParallelism)  
}

/** Case class wrapping the number of partitions to use when parallelizing sequences
 *  to Spark, a type different from Int allows us to be more specified when requiring
 *  implicit parameters in functions
 * */
case class Parallelism(val numSlices : Int)
