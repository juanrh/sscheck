package es.ucm.fdi.sscheck.gen

import org.scalacheck.{Properties, Gen}
import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.Prop.{forAll, BooleanOperators, exists, atLeastOne}

object ReGenTest extends Properties("ReGen regex generators properties") {
 
  property("epsilon generates empty sequences") = 
    forAll (ReGen.epsilon) { (xs : Seq[Int]) =>
  	  xs.length == 0
  	}
  
  property("symbol generates a single element that contains the argument") = 
    forAll { x : String => 
      forAll (ReGen.symbol(x)) { xs : Seq[String] =>
        xs.length == 1 && xs(0) == x
      }
  }
  
   // def alt[A](gs : Gen[Seq[A]]*) : Gen[Seq[A]] = {
  property("alt is equivalent to epsilon if zero generators are provided") = 
    forAll (ReGen.alt()) { (xs : Seq[Int]) =>
      forAll (ReGen.epsilon) { (ys : Seq[Int]) =>
        xs == ys
      }
  }
  
  property("alt works for more than one argument, and generates values for some of the alternatives (weak, existential)") =  {
    val (g1, g2, g3) = (ReGen.symbol(0), ReGen.symbol(1), ReGen.symbol(2))
    forAll (ReGen.alt(g1, g2, g3)) { (xs : Seq[Int]) =>
      atLeastOne(
        exists (g1) { (ys : Seq[Int]) =>
        xs == ys
        },
        exists (g2) { (ys : Seq[Int]) =>
        xs == ys
        },
        exists (g3) { (ys : Seq[Int]) =>
        xs == ys
        }
      )
    }
  }

  // conc and star only have similar weak existential properties
}