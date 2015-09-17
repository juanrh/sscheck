package es.ucm.fdi.sscheck.gen

import org.scalacheck.util.Buildable
import scala.language.{implicitConversions,higherKinds,postfixOps}

object Buildables {
  /** Buildable for Seq, so we can use Gen.containerOf and Arbitrary.arbitrary
  *  using Seq as a container
  *  */
  implicit def buildableSeq[T] = new Buildable[T, Seq[T]] {
    def builder = new collection.mutable.Builder[T,Seq[T]] {
    var xs : List[T] = List() 
      def +=(x: T) = {
       xs = x :: xs
       this
      }
      def clear = xs = List()
      def result = xs reverse // note it is important to respect the generation order
    }
  }
    
  /** Builds a Buildable as a transformation of a given buildable, both element wise and
  *  on the container
  * */
  def mapBuildable[T1, C1[_], T2, C2[_]]
    (elemMapping : T2 => T1, resultMapping : C1[T1] => C2[T2])(buildable : Buildable[T1, C1[T1]])
    : Buildable[T2, C2[T2]] = new Buildable[T2, C2[T2]] { 
      def builder = new collection.mutable.Builder[T2,C2[T2]] {
        var nestedBuilder : collection.mutable.Builder[T1, C1[T1]] = buildable.builder
        def +=(x : T2) = {
          nestedBuilder += elemMapping(x)
          this
        }
        def clear = nestedBuilder.clear
        def result = nestedBuilder.mapResult(resultMapping).result
      }
  }
  /** Builds a Buildable as a transformation of a given buildable, by tranforming 
   *  the result with collection.mutable.Builder.mapResult
   * */
  def mapBuildable[T, R1, R2]
    (resultMapping : R1 => R2)(buildable : Buildable[T, R1])
    : Buildable[T, R2] = new Buildable[T, R2] { 
      def builder = new collection.mutable.Builder[T,R2] {
      var nestedBuilder : collection.mutable.Builder[T, R1] = buildable.builder
        def +=(x : T) = {
          nestedBuilder += x
          this
        }
        def clear = nestedBuilder.clear
        def result = nestedBuilder.mapResult(resultMapping).result
      }
  }
    
  /** A Buildable for building an object Batch[T] from its elements of type T
   * */
  implicit def buildableBatch[T] : Buildable[T, Batch[T]] = {      
    /* alternative implementation based on the overload of mapBuildable, implies additional
     * calls to identity
   
     mapBuildable(identity[T], (xs : List[T]) => Batch(xs))(implicitly[Buildable[T, List[T]]])
     */
    mapBuildable((xs : List[T]) => Batch(xs:_*))(implicitly[Buildable[T, List[T]]])
  }
    
  /** A Buildable for building an object DStream[T] from its batches of type Batch[T]
   * */
  implicit def buildablePDStreamFromBatch[T] : Buildable[Batch[T], PDStream[T]] = 
    mapBuildable((batches : List[Batch[T]]) => PDStream(batches:_*))(
                 implicitly[Buildable[Batch[T], List[Batch[T]]]])
  
  
}