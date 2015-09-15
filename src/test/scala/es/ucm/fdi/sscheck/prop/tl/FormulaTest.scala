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
  //with MustThrownExpectations TODO: adapt test to functional specification, create issue
  {
  
  def is = sequential ^ s2"""
    Basic test for temporal logic formulas representation
      - where some example formulas are correctly built $exampleFormulas
      - where nextFormula is defined correctly $nextFormulaOk
      - where examples from the paper for nextFormula work as expected $nextFormulaPaper 
      - where evaluation with consume works correclty $consumeOk
      - where safeWordLength is ok $pending
    """    
      
  // Consider an universe with an Int i and a String s
  type U = (Int, String)
  type Form = Formula[U]
  val (i, s) = ((_ : U)._1, (_ : U)._2)
  // some atomic propositions
  val aP : Form = at(i)(_  must be_>(2))
  val aQ = at(s)(_ contains "hola")
      
  def exampleFormulas = {         
    val notP = ! aP
    val pImpliesQ = aP ==> aQ
    val nextP = next (aP)
    val alwaysP = always (aP) during 6
    
    // note there are no exceptions due to suspended evaluation
    // Now ((x : Any) => 1 === 0)
    
    val pUntilQ = aP until aQ on 4
    val pUntilQExplicitTimeout = aP until aQ on Timeout(4)
    val nestedUntil = (aP until aQ on 4) until aP on 3
    val pUntilQImplicitTimeout : Form = {
      implicit val t = Timeout(3)
      aP until aQ
    }
    
    aP must not be_==(aQ) and 
    // TODO: add examples for each of the case classes
    ok
  }
  
  def nextFormulaOk = {
    // now
    { aP. nextFormula === aP } and
    { aP. nextFormula must not be_==(aQ) } and 
    // solved
    { 
      val solvedP = aP. nextFormula. consume((3, "hola")) 
      solvedP. nextFormula === solvedP 
    } and
    //
    // not
    { (! aP). nextFormula === !(aP. nextFormula) } and
    // or
    { (aP or aQ). nextFormula === (aP. nextFormula or aQ. nextFormula) } and
    // and 
    { (aP and aQ). nextFormula === (aP. nextFormula and aQ. nextFormula) } and
    // implies: note it is reduced to an or
    { (aP ==> aQ). nextFormula === (! aP. nextFormula or aQ. nextFormula) } and
    //
    // next
    { next(aP). nextFormula === next(aP. nextFormula) } and
    //
    // eventually
    { (later(aP) on 1). nextFormula === aP } and
    { (later(aP) on 2). nextFormula === (aP or next(aP)) } and 
    { (later(aP) on 3). nextFormula === or(aP, next(aP), next(next(aP))) } and 
    //
    // always
    { (always(aP) during 1). nextFormula === aP } and
    { (always(aP) during 2). nextFormula === (aP and next(aP)) } and 
    { (always(aP) during 3). nextFormula === and(aP, next(aP), next(next(aP))) } and
    //
    // until
    { (aP until aQ on 1). nextFormula === aQ } and
    { (aP until aQ on 2). nextFormula === or(aQ, and(aP, next(aQ))) } and 
    { (aP until aQ on 3). nextFormula === 
      or(aQ, and(aP, next(aQ)), and(aP, next(aP), next(next(aQ)))) } and
    //
    // release
    { (aP release aQ on 1).nextFormula === or(aQ, and(aP, aQ)) } and
    { (aP release aQ on 2).nextFormula === 
      or(and(aQ, next(aQ)), 
         and(aP, aQ),
         and(aQ, next(aP), next(aQ))
      ) } and
    { (aP release aQ on 3).nextFormula ===
      or(and(aQ, next(aQ), next(next(aQ))), 
         and(aP, aQ),
         and(aQ, next(aP), next(aQ)),
         and(aQ, next(aQ), next(next(aP)), next(next(aQ)))
      ) }
  }
  
  def nextFormulaPaper = {
    val phi = always (aQ ==> (later(aP) on 2)) during 2
    println(s"phi.nextFormula ${phi.nextFormula}")
    phi.nextFormula ===  
     ( (!aQ or (aP or next(aP))) and 
       next(!aQ or (aP or next(aP))) )
  }
  
  def consumeOk = {
    type U = (Int, Int)
    type Form = Formula[U]
    // some atomic propositions
    val a : Form = (u : U) => u._1 must be_> (0)
    val b : Form = (u : U) => u._2 must be_> (0)
    // some letters
    val aL : U = (1, 0) // only a holds
    val bL : U = (0, 1) // only b holds
    val abL : U = (1, 1) // both a and b hold
    val phi = always (b ==> (later(a) on 2)) during 2
    val phiNF = phi.nextFormula
    println(s"phiNF: $phiNF")
    
    { phiNF.getResult === None } and 
    {
      val phiNF1 = phiNF consume(bL)
      println(s"phiNF1: $phiNF1")
      val phiNF2 = phiNF1 consume(bL)
      println(s"phiNF2: $phiNF2")
      //phiNF1.getResult must be(None)
      println(phiNF2 consume(bL))
      pending
    }
  }
}