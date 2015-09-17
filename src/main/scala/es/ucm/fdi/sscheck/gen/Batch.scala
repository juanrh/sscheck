package es.ucm.fdi.sscheck.gen

import scala.collection.immutable.{HashBag=>Bag}
import scala.language.implicitConversions

object Batch {
  def empty[A] : Batch[A] = new Batch(points = List():_*)
    
  implicit def seq2batch[A](seq : Seq[A]) : Batch[A] = Batch(seq:_*)
}

/** Objects of this class represent batches of elements 
 *  in a discrete data stream 
 * */
case class Batch[A](points : A*) extends Seq[A] {
  implicit val config = Bag.configuration.compact[A]
  
  override def toSeq : Seq[A] = points
  def toBag : Bag[A] = Bag(points:_*) 
  
  override def apply(idx : Int) = points.apply(idx)
  override def iterator = points.iterator
  override def length = points.length
  
}