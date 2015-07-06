package es.ucm.fdi.sscheck.prop

import org.scalacheck.{Properties, Gen}
import org.scalacheck.Prop.{forAll, exists, AnyOperators}
import org.scalacheck.Prop
import org.scalatest._
import org.scalatest.Matchers._
import org.specs2.matcher.MatchFailureException
import scala.util.{Try, Success, Failure}
import org.scalacheck.util.Pretty

object UtilsProp {
  def safeProp[P <% Prop](p : => P) : Prop = {
    Try(p) match {
      case Success(pVal) => pVal
      case Failure(t) => t match {
        case _: TestFailedException => Prop.falsified
        case _: MatchFailureException[_] => Prop.falsified
        case _ => Prop.exception(t) 
      }
    }
  }
  
  def safeExists[A, P](g: Gen[A])(f: (A) => P)
                      (implicit pv: (P) => Prop, pp: (A) => Pretty): Prop = {    
    exists(g)((x : A) => safeProp(pv(f(x))))(identity[Prop], pp)
    // This doesn't work for reasons unknown
    // exists(g)((x : A) => Prop.secure(pv(f(x))))(identity[Prop], pp)
  }  
}