package es.ucm.fdi.sscheck.prop.tl

import org.scalacheck.Gen
import org.scalacheck.Arbitrary.arbitrary

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.ScalaCheck
import org.specs2.Specification
import org.specs2.matcher.MustThrownExpectations
import org.specs2.execute.Result

import Formula._

@RunWith(classOf[JUnitRunner])
class FormulaTest
  extends Specification 
  with MustThrownExpectations  {
  
  def is = sequential ^ s2"""
    Basic test for temporal logic formulas representation
      - where some example formulas are correctly built $exampleFormulas
      - where nextFormula is defined correctly $nextFormulaOk
      - where safeWordLength is ok $pending
    """    
      
  // Consider an universe with an Int i and a String s
  type U = (Int, String)
  type Form = Formula[U]
  val (i, s) = ((_ : U)._1, (_ : U)._2)
  // some atomic propositions
  val aP = at(i)(_  must be_>(2))
  val aQ = at(s)(_ contains "hola")
      
  def exampleFormulas = {         
    val notP = ! aP
    val pImpliesQ = aP ==> aQ
    val nextP = next (aP)
    val alwaysP = always (aP) during 6
    
    // note there are no exceptions due to suspended evaluation
    Now ((x : Any) => 1 === 0)
    
    val pUntilQ = aP until aQ on 4
    val pUntilQExplicitTimeout = aP until aQ on Timeout(4)
    val nestedUntil = (aP until aQ on 4) until aP on 3
    val pUntilQImplicitTimeout : Form = {
      implicit val t = Timeout(3)
      aP until aQ
    }
   // TODO: add examples for each of the case classes
    ok
  }
  
  def nextFormulaOk = {
    (later(aP) on 1). nextFormula === aP
    (later(aP) on 2). nextFormula === (aP or next(aP)) 
    (later(aP) on 3). nextFormula === (aP or next(aP) or next(next(aP)))
    
    (always(aP) during 1). nextFormula === aP
    (always(aP) during 2). nextFormula === (aP and next(aP)) 
    (always(aP) during 3). nextFormula === (aP and next(aP) and next(next(aP)))
    
    // TODO: add assertions for each of the case classes 
  }
}