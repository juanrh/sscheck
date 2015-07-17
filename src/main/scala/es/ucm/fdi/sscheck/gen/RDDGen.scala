package es.ucm.fdi.sscheck.gen

import org.scalacheck.Gen

import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import es.ucm.fdi.sscheck.spark.Parallelism

/** Generators for RDDs and implicit conversions from Seq and Seq generators to RDD and RDD 
 *  generators. All the functions are based on parallelizing generated sequences
 *  
 *  Instead of defining a class holding a spark context and a [[es.ucm.fdi.sscheck.testing.Parallelism]]
 *  object specifying the number of partitions to use on parallelization, these have to be explicitly
 *  provided when calling the methods, although these arguments have been made implicit to simulate
 *  state if wanted, like it is done in [[es.ucm.fdi.sscheck.testing.SharedSparkContextBeforeAfterAll]]  
 * */
object RDDGen { 
  /** Convert a ScalaCheck generator of Seq into a generator of RDD   
   * */
  implicit def seqGen2RDDGen[A](sg : Gen[Seq[A]])
              (implicit aCt: ClassTag[A], sc : SparkContext, parallelism : Parallelism) : Gen[RDD[A]] =
    sg.map(sc.parallelize(_, numSlices = parallelism.numSlices))
  
  /** Convert a sequence into a RDD    
   * */
  implicit def seq2RDD[A](seq : Seq[A])(implicit aCt: ClassTag[A], sc : SparkContext, parallelism : Parallelism) : RDD[A] = 
    sc.parallelize(seq, numSlices=parallelism.numSlices)
    
  /** @returns a generator of RDD that generates its elements from g
   * */
  def of[A](g : => Gen[A])
           (implicit aCt: ClassTag[A], sc : SparkContext, parallelism : Parallelism) 
           : Gen[RDD[A]] = 
    // this way is much simpler that implementing this with a ScalaCheck
    // Buildable, because that implies defining a wrapper to convert RDD into Traversable
    seqGen2RDDGen(Gen.listOf(g))
    
  /** @returns a generator of RDD that generates its elements from g
  * */
  def ofN[A](n : Int, g : Gen[A])
  			(implicit aCt: ClassTag[A], sc : SparkContext, parallelism : Parallelism)
  			: Gen[RDD[A]] = {
    seqGen2RDDGen(Gen.listOfN(n, g))
  }
  
   /** @returns a generator of RDD that generates its elements from g
  * */
  def ofNtoM[A](n : Int, m : Int, g : => Gen[A]) 
  			   (implicit aCt: ClassTag[A], sc : SparkContext, parallelism : Parallelism)
  			   : Gen[RDD[A]] = 
    seqGen2RDDGen(UtilsGen.containerOfNtoM[List, A](n, m, g))
}

