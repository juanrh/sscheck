package es.ucm.fdi.sscheck.gen

import org.scalacheck.Gen

import es.ucm.fdi.sscheck.gen.UtilsGen.containerOfNtoM;
import Buildables.buildableSeq
import UtilsGen.{containerOfNtoM}

import scala.language.postfixOps

/**
  Generators for simple regexp, as generators of Seq[A]
  */
object ReGen {
  
  val epsilon = Gen.const(List()) 
  
  def symbol[A](s : A) : Gen[Seq[A]] = Gen.const(List(s))
  
  // def alt[A](g1 : Gen[Seq[A]], g2 : Gen[Seq[A]]) : Gen[Seq[A]] = Gen.oneOf(g1, g2)
  def alt[A](gs : Gen[Seq[A]]*) : Gen[Seq[A]] = {
    val l = gs.length
    // require (l > 0, "alt needs at least one alternative")
    if (l == 0)
       epsilon
  	else if (l == 1)
  		gs(0)
  	else
      Gen.oneOf(gs(0), gs(1), gs.slice(2, l):_*)
  }   
  
  def conc[A](g1 : Gen[Seq[A]], g2 : Gen[Seq[A]]) : Gen[Seq[A]] = {
     for {
       xs <- g1
       ys <- g2
     } yield xs ++ ys
  }
  
  def star[A](g : Gen[Seq[A]]) : Gen[Seq[A]] = {
    for {
	  xs <- Gen.containerOf(g)
	} yield xs flatten
  }    
}