package es.ucm.fdi.sscheck.testing

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.scalacheck.{Parameters, ScalaCheckProperty}
import org.specs2.specification.BeforeAfterEach

import org.scalacheck.{Prop, Gen}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.AnyOperators

import com.typesafe.scalalogging.slf4j.Logging

// (almost the same) example for https://github.com/etorreborre/specs2/issues/393
@RunWith(classOf[JUnitRunner])
class SequentialScalacheckOriginal extends org.specs2.Specification 
                     with org.specs2.matcher.MustThrownExpectations
                     with BeforeAfterEach
                     with ScalaCheck 
                     with Logging {
  def is = 
   sequential ^
   "Example run should be sequential with ScalaCheck configured for one worker" ^
     "property 1"  ! skipped // this generates and error before Specs2 v "3.6.2-20150716123420-ac2f605" prop1 ^ 
     "property 2"  ! prop2 ^
     end

  var counters = scala.collection.mutable.Map("example" -> 0, "test case" -> 0, "prop1" -> 0)
  def getPrinter(counterId : String) = { 
    if (counterId == "test case")
      (s : String) => logger.debug(s)
    else 
      (s : String) => logger.warn(s)		    	
  }

  def enter(counterId : String) : Unit = this.synchronized {
    val print = getPrinter(counterId)
    print(s"one more $counterId")
    counters(counterId) = counters(counterId) + 1
    if (counters(counterId) > 1) {
      logger.error(s"too many $counterId")
      throw new RuntimeException("this should be sequential")
    }
  }

  def exit(counterId : String) : Unit = this.synchronized { 
    getPrinter(counterId)(s"one less $counterId")
    counters(counterId) -= 1
  }

  var once = false
  def enterOnce() : Unit = this.synchronized {
    if (once) {
      throw new RuntimeException("this should be executed just once")
    }
    once = true
  }

  override def before : Unit = enter("example")
  override def after : Unit = exit("example")

  def prop1 = {
    enter("prop1") 
    enterOnce()
    val p = 
      Prop.forAll ("x" |: Gen.choose(0, 100)) { x : Int => 
        logger.debug(s"$x,")
        enter("test case")
        x must be_>=(0)
        exit("test case")
        true
    } . set(workers = 1, minTestsOk = 150).verbose // comment the set and this works ok
   exit("prop1")
   p
  }

  def prop2 = {
    logger.info("running prop2")
    2 === 2
  }
}