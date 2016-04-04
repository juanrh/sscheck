package es.ucm.fdi.sscheck.gen

import org.scalatest.matchers.{Matcher, MatchResult}
import scala.language.implicitConversions

object PDStream {    
  def empty[A] : PDStream[A] = new PDStream(List():_*)
    
  implicit def batchSeq2dstream[A](batches : Seq[Batch[A]]) : PDStream[A] = PDStream(batches:_*)
  implicit def seqSeq2dstream[A](batches : Seq[Seq[A]]) : PDStream[A] = PDStream(batches.map(Batch(_:_*)):_*)
}

/** An object of this class represents a finite prefix of a discrete data streams, 
 *  aka prefix DStream or just PDStream  
 * */
case class PDStream[A](batches : Batch[A]*) extends Seq[Batch[A]] {    
  override def toSeq : Seq[Batch[A]] = batches
  
  override def apply(idx : Int) = batches.apply(idx)
  override def iterator = batches.iterator
  override def length = batches.length
  
  // Note def ++(other : DStream[A]) : DStream[A] is inherited from Seq[_]
    
  /** @return a DStream for the batch-by-batch concatenation of this
  *  and other. Note we fill both PDStreams with empty batches. This 
  *  implies both PDStreams are implicitly treated as they where 
  *  infinitely extended with empty batches  
  */
  def #+(other : PDStream[A]) : PDStream[A] = {
    batches. zipAll(other, Batch.empty, Batch.empty)
           . map(xs12 => xs12._1 ++ xs12._2)
  }

  /** @return true iff each batch of this dstream is contained in the batch
   *  at the same position in other. Note this implies that true can be
   *  returned for cases when other has more batches than this
   * */
  def subsetOf(other : PDStream[A]) : Boolean = {
    batches
      .zip(other.batches)
      .map({case (thisBatch, otherBatch) =>
              thisBatch.forall(otherBatch.contains(_))
           })
      .forall(identity[Boolean])
  } 
}

trait DStreamMatchers {
  class DStreamSubsetOf[A](expectedSuperDStream : PDStream[A]) extends Matcher[PDStream[A]] {
    override def apply(observedDStream : PDStream[A]) : MatchResult = {      
      // FIXME reimplement with Inspector for better report
      MatchResult(observedDStream.subsetOf(expectedSuperDStream), 
      			s"""$observedDStream is not a pointwise subset of $expectedSuperDStream""", 
      			s"""$observedDStream is a pointwise subset of $expectedSuperDStream""")
    }
  }
  def beSubsetOf[A](expectedSuperDStream : PDStream[A]) = new DStreamSubsetOf(expectedSuperDStream)
}
object DStreamMatchers extends DStreamMatchers
