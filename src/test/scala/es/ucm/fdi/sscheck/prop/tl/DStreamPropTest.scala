package es.ucm.fdi.sscheck.prop.tl

import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.MustThrownExpectations
import org.specs2.execute.Result

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.dstream.DStream

import es.ucm.fdi.sscheck.spark.streaming.SharedStreamingContextBeforeAfterEach
import Formula._

@RunWith(classOf[JUnitRunner])
class DStreamPropTest 
  extends Specification  
  with SharedStreamingContextBeforeAfterEach
  with ScalaCheck {
  
  // FIXME: change to not so small test sizes and number of tests
  override def sparkMaster : String = "local[5]"
  override def batchDuration = Duration(350)
  override def defaultParallelism = 4  
  
    def is = sequential ^ s2"""
    Basic test for properties with temporal logic formulas
      - where a simple forall always prop works ok ${countForallAlwaysProp(_.count)}   
    """    
  //  - where FIXME $pending exampleFormulaProp
  
  def exampleFormulaProp = {    
    DStreamProp.forAll(
      Gen.listOfN(10,  Gen.listOfN(30, arbitrary[Double])))(
        _.map(_.toString)){
      type Ats = (RDD[Double], RDD[String])
      val inB = (_ : Ats)._1
      val outB = (_ : Ats)._2
      val as1 = at(inB)(_.count === 30)
      val as2 = at(outB)(_.count === 30)
      //val as12 = w(identity[Ats])((rI, rO) => rI.count === rO.count) // this is quite useless
      val as12 : Formula[Ats] = (ats : Ats) => inB(ats).count === inB(ats).count // more useful use of projections
      
      at(inB)(_.count === 30) until at(inB)(_.count === 30) on 2
      val r = at(inB)(_.count === 30) until 
                { (ats : Ats) => inB(ats).count === inB(ats).count } on 3
      r
    }
    ok
  } 
  
  def countForallAlwaysProp(testSubject : DStream[Double] => DStream[Long]) = {
    type U = (RDD[Double], RDD[Long])
    val (inBatch, transBatch) = ((_ : U)._1, (_ : U)._2)
    val numBatches = 3 // 10
    val formula : Formula[U] = always { (u : U) =>
      transBatch(u).count === 1 and
      inBatch(u).count === transBatch(u).first 
    } during numBatches // TODO numBatches+1 leads to undecided, add test for this
    
    DStreamProp.forAll(
      Gen.listOfN(numBatches, Gen.listOfN(2, arbitrary[Double])))( // FIXME restore 30
      testSubject)(
      formula)
      .set(minTestsOk = 2).verbose
  }

}