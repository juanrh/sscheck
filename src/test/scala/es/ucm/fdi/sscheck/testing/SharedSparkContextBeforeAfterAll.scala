package es.ucm.fdi.sscheck.testing

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner 

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.specs2.scalacheck.Parameters

import org.scalacheck.{Prop, Gen}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.AnyOperators

import org.apache.spark._
import org.apache.spark.rdd.RDD

import es.ucm.fdi.sscheck.gen.RDDGen
import es.ucm.fdi.sscheck.gen.RDDGen._

import scala.reflect.ClassTag

@RunWith(classOf[JUnitRunner])
class SharedSparkContextForEachTest extends Specification
		 						    with SharedSparkContextBeforeAfterAll 
		 						    with ScalaCheck {

  override def defaultParallelism: Int = 3
  override def master : String = "local[5]"
  override def appName = this.getClass().getName()
  
  implicit def defaultScalacheckParams = Parameters(minTestsOk = 101).verbose
  
  "Sharing a Spark Context between several ScalaCheck properties and test cases, and closing it properly".title ^
    "forall that ignores the Spark context" ! forallOkIgnoreSparkContext
	"simple test that uses the Spark context explicitly" ! simpleExplicitUseSparkContext(sc) 
	"forall that uses the Spark context explicitly, and parallelizes a Seq explicitly" ! forallSparkContextAndParallelizeExplicit(sc) 
	"forall that uses the Spark context from this, and parallelizes a Seq explicitly" ! forallSparkContextFromThisAndParallelize
	"forall that parallelizes a Seq with an implicit" ! forallSparkContextAndParallelizeImplicit
	"forall with implicit conversion of Seq generator to RDD generator" ! forallImplicitToRDDGen
	"forall that uses RDDGen.of" ! forallRDDGen
	"forall that uses RDDGen.of with local overload of parallelism" ! forallRDDGenOverloadPar
	"forall that uses RDDGen.ofNtoM" ! forallRDDGenOfNtoM
	"forall that uses RDDGen.ofN, testing frequency generator" ! forallRDDGenOfNFreqMean
		
  def forallOkIgnoreSparkContext = Prop.forAll { x : Int => 
    x + x === 2 * x 
  }
  
  def simpleExplicitUseSparkContext(sc : SparkContext) = {
    sc.parallelize(1 to 10).count === 10
  }
  
  def fooCheckOnSeqAndRDD(batch : Seq[Double], rdd : RDD[Double]) = {
    val plusOneRdd = rdd.map(_+1)
    if (plusOneRdd.count > 0)
      plusOneRdd.sum must be_>= (batch.sum - 0.0001) // substraction to account for rounding
    rdd.count() ?= batch.length
  }
  
  def forallSparkContextAndParallelizeExplicit(sc : SparkContext) = 
    Prop.forAll ("batch" |: Gen.listOf(Gen.choose(-1000.0, 1000.0))) { batch : List[Double] =>
      // take Spark Context from argument and parallelize explicitly
      val rdd = sc.parallelize(batch)
      fooCheckOnSeqAndRDD(batch, rdd)
    }. set(minTestsOk = 50).verbose
 
  def forallSparkContextFromThisAndParallelize = 
    Prop.forAll ("batch" |: Gen.listOf(Gen.choose(-1000.0, 1000.0))) { batch : List[Double] =>
      // take Spark context from this and parallelize explicitly
      val rdd = sc.parallelize(batch)
      fooCheckOnSeqAndRDD(batch, rdd)
    }. set(minTestsOk = 50).verbose
    
  def forallSparkContextAndParallelizeImplicit = 
    Prop.forAll ("batch" |: Gen.listOf(Gen.choose(-1000.0, 1000.0))) { batch : List[Double] =>
      // parallelize implicitly 
      val rdd : RDD[Double] = batch 
      fooCheckOnSeqAndRDD(batch, rdd)
    }. set(minTestsOk = 50).verbose
      
  def forallImplicitToRDDGen = 
    /* NOTE the context below doesn't force the generator to be a Gen[RDD[Int]], so 
       we have to force it explicitly
         Prop.forAll (Gen.listOf(0) ) { xs : RDD[Int] =>
    */
    Prop.forAll ("rdd" |: Gen.listOf(Gen.choose(-100, 100)) : Gen[RDD[Int]]) { xs : RDD[Int] => 
      val xsSum = xs.sum
      xsSum ?= xs.map(_+1).map(_-1).sum
      xsSum + xs.count must beEqualTo(xs.map(_+1).sum)
    }. set(minTestsOk = 50).verbose 
   
  def forallRDDGen =
    Prop.forAll("rdd" |: RDDGen.of(Gen.choose(-100, 100))) { rdd : RDD[Int] =>
      rdd.count ===  rdd.map(_+1).count
    }. set(minTestsOk = 10).verbose 
    
  def forallRDDGenOverloadPar = {
    // note here we have to use "parallelism" as the name of the val
    // in order to shadow this.parallelism, to achieve its overloading
    val parLevel = 5
    implicit val parallelism = Parallelism(numSlices=parLevel)
    Prop.forAll("rdd" |: RDDGen.of(Gen.choose(-100, 100))){ rdd : RDD[Int] =>
      parLevel === rdd.partitions.length 
    }. set(minTestsOk = 10).verbose 
  }

  def forallRDDGenOfNtoM = {
    val minWords, maxWords = (50, 100)
    Prop.forAll(RDDGen.ofNtoM(50, 100, arbitrary[String])) { rdd : RDD[String] =>
      rdd.map(_.length()).sum must be_>=(0.0)  
    }
  }
  
  def forallRDDGenOfNFreqMean = {
    val freqs = Map(1 -> 0, 4 -> 1)
    val rddSize = 200
    val gRDDFreq = RDDGen.ofN(rddSize, Gen.frequency(freqs.mapValues(Gen.const(_)).toSeq:_*))
    val expectedMean = {
      val freqS = freqs.toSeq
      val num = freqS .map({case (f, v) => v * f}). sum
      val den = freqS .map(_._1). sum
      num / den.toDouble
    }  
    Prop.forAll("rdd" |: gRDDFreq){ rdd : RDD[Int] =>
      rdd.mean must be ~(expectedMean +/- 0.1) 
    }
  }. set(minTestsOk = 50).verbose 
}