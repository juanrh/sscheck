package es.ucm.fdi.sscheck.prop.tl

import org.specs2.execute.{Result,AsResult}
import org.specs2.scalacheck.AsResultProp
import org.scalacheck.Prop

import scalaz.syntax.std.boolean._

import scala.annotation.tailrec
import scala.language.{postfixOps,implicitConversions}
object Formula {
  /** More succinct notation for timeouts when combined with TimeoutMissingFormula.on()  
   */
  implicit def intToTimeout(t : Int) : Timeout = Timeout(t)
  /** Completes a TimeoutMissingFormula when a timeout is implicitly available
   */
  implicit def timeoutMissingFormulaToFormula[T](f : TimeoutMissingFormula[T])
                                                (implicit t : Timeout) : Formula[T] = f.on(t)
  /** More succinct notation for Now formulas
   */
  implicit def resultFunToNow[T, R <% Result](p : T => R): Now[T] = Now(p)
 // implicit def propStatusFunToNow[T](p : T => Prop.Status) : Now[T] = Now.fromStatusFun(p)
  implicit def resultFunToFormulaFun[T, R <% Result](p: T => R): (T => Formula[T]) =
    // p andThen Now.resultToPropStatus _ andThen Solved.apply _
    p andThen implicitly[Function[R,Result]] andThen Now.resultToPropStatus _ andThen Solved.apply _
    
    // new Now[T](p andThen Solved.apply _)
    //     fromStatusFun(p andThen implicitly[Function[R,Result]] andThen resultToPropStatus _)

  /*
   * FIXME move to suitable place
   * Could use Shapeless coproducts / type unions https://github.com/milessabin/shapeless for this
   * requiring a type union in the argument and then returnign different result depending on
   * the matched type. But that would require macro paradise as this is scala 2.10, and in
   * general it introduces a complex depenency that would make this library weaker
   * */
  
  /** Builds a Now formula of type T by composing the projection proj on 
   *  T with an the assertion function p
   */
  def at[T, A, R <% Result](proj : (T) => A)(p : A => R): Now[T] = Now(p compose proj)
  
  /** Build a Now formula of type T, useful for defining a type context to define assertion
   *  with a partial function literal
   * */
  // FIXME: don't like the name much, same problem with type erasure as Now.apply
  def now[T](assertion: T => Formula[T]): Formula[T] = Now(assertion)
  /** Build a Now formula of type T, useful for defining a type context to define assertion
   *  with a partial function literal
   * */
  // FIXME: don't like the name much, same problem with type erasure as Now.apply
 // def nowR[T](assertion: T => Result): Formula[T] = Now(assertion) // <- this should be covered by resultFunToNow
  /** Build a Now formula of type T, useful for defining a type context to define assertion
   *  with a partial function literal
   * */
  // FIXME: don't like the name much, same problem with type erasure as Now.apply
 // def nowS[T](assertion: T => Prop.Status): Formula[T] = Now.fromStatusFun(assertion)

  // builders for non temporal connectives: note these act as clever constructors
  // for Or and And
  def or[T](phis : Formula[T]*): Formula[T] =
    if (phis.length == 0) Solved(Prop.False)
    else if (phis.length == 1) phis(0) 
    else Or(phis:_*)
  def and[T](phis : Formula[T]*): Formula[T] = 
    if (phis.length == 0) Solved(Prop.True)
    else if (phis.length == 1) phis(0) 
    else And(phis:_*)
    
  // builders for temporal connectives
  def next[T](phi : Formula[T]) = Next(phi)
  def eventually[T](phi : Formula[T]) = new TimeoutMissingFormula[T](Eventually(phi, _))
  /** Alias of eventually that can be used when there is a name class, for example 
   *  with EventuallyMatchers.eventually 
   * */
  def later[T](phi : Formula[T]) = eventually(phi)
  def always[T](phi : Formula[T]) = new TimeoutMissingFormula[T](Always(phi, _))  
  
  // FIXME don't mix concerns: first solve the quantifier problem and then 
  // start with the partial function notation. now() can be used for the first concern to start 
  // FIXME should rename just to always so 1) allows for quantified formulas, 
  // 2) allows partial function notation instead of at <-- or maybe not, note
  // - functions to Result should be formulas to Solved: as there is no need to consume 
  // more letters, and note Now consumes 1 letter
  // - functions to Formula are just that
  // - Now should be able to be used to define quantified formulas: think how
//  def alwaysLQuant[T](a: T => Result): TimeoutMissingFormula[T] =
//    always(Now(a(_))) // this makes sense, but should be replaced by a generalization where Now doesn't exist TODO
//  def alwaysLQuant2[T](a: T => Formula[T]): TimeoutMissingFormula[T] =
//    always(NowLQuant(a))
    
}

// using trait for the root of the AGT as recommended in http://twitter.github.io/effectivescala/
sealed trait Formula[T] 
  extends Serializable {
  
  def safeWordLength : Timeout
  def nextFormula : NextFormula[T]
  
  // non temporal builder methods
  def unary_! = Not(this)
  def or(phi2 : Formula[T]) = Or(this, phi2)
  def and(phi2 : Formula[T]) = And(this, phi2)
  def ==>(phi2 : Formula[T]) = Implies(this, phi2)
  // temporal builder methods: next, eventually and always are methods of the Formula companion object
  def until(phi2 : Formula[T]) = new TimeoutMissingFormula[T](Until(this, phi2, _))
  def release(phi2 : Formula[T]) = new TimeoutMissingFormula[T](Release(this, phi2, _))
}

/** Restricted class of formulas that are in a form suitable for the 
 *  formula evaluation procedure
 */
sealed trait NextFormula[T] 
  extends Formula[T] {
  /** @return Option.Some if this formula is resolved, and Option.None
   *  if it is still pending resolution when some additional values 
   *  of type T corresponding to more time instants are provided with
   *  a call to consume()
   * */
  def result : Option[Prop.Status] 
  
  /** @return a new formula resulting from progressing in the evaluation 
   *  of this formula by consuming the new values atoms for the atomic 
   *  propositions corresponding to the values of the element of the universe
   *  at a new instant of time. This corresponds to the notion of "letter simplification" 
   *  in the paper
   */
  def consume(atoms : T) : NextFormula[T]
}

/** Resolved formulas
 * */
// see https://github.com/rickynils/scalacheck/blob/1.12.2/src/main/scala/org/scalacheck/Prop.scala
case class Solved[T](res : Prop.Status) extends NextFormula[T] {
  override def safeWordLength = Timeout(0)
  override def nextFormula = this
  override def result = Some(res)
  // do no raise an exception in call to consume, because with NextOr we will
  // keep undecided prop values until the rest of the formula in unraveled
  override def consume(atoms : T) = this
}

/* FIXME
 * def union[T](t: T)(implicit c: (T =:= String) Or (T =:= Int)) = t match {
  case s: String => println(s"Some nice string: $s")
  case i: Int => println(s"Some int: $i")
}
type V[A, B] = {type l[T] = (T <:< A) Or (T <:< B)}

 * */

/** Formulas that have to be resolved now, which correspond to atomic proposition
 *  as functions from the current state of the system to Specs2 assertions. 
 *  Note this also includes top / true and bottom / false as constant functions
 */
object Now { 
  /* from now going for avoiding the type erasure problem, TODO check more sophisticated
   * solutions like http://hacking-scala.org/post/73854628325/advanced-type-constraints-with-type-classes 
   * or http://hacking-scala.org/post/73854628325/advanced-type-constraints-with-type-classes based or
   * using ClassTag. Also why is there no conflict with the companion apply?
   */
  //def fromStatusFun[T](p : T => Prop.Status): Now[T] =
  //  new Now[T](p andThen Solved.apply _)
  //def apply[T, R <% Result](p : T => R): Now[T] = 
  //  fromStatusFun(p andThen implicitly[Function[R,Result]] andThen resultToPropStatus _)
    
  /** Convert a Specs2 Result into a ScalaCheck Prop.Status
   * See 
   * - trait Status: https://github.com/rickynils/scalacheck/blob/1.12.2/src/main/scala/org/scalacheck/Prop.scala
   * - subclasses of Result: https://etorreborre.github.io/specs2/api/SPECS2-3.6.2/index.html#org.specs2.execute.Result
   * and https://etorreborre.github.io/specs2/guide/SPECS2-3.6.2/org.specs2.guide.StandardResults.html
   * */
  @tailrec
  def resultToPropStatus(result : Result) : Prop.Status = result match {
    case err : org.specs2.execute.Error => Prop.Exception(err.t)
    case _ : org.specs2.execute.Failure => Prop.False
    case _ : org.specs2.execute.Pending => Prop.Undecided
    case _ : org.specs2.execute.Skipped => Prop.Undecided
    /* Prop.True is the same as passed, see lazy val passed 
    * at https://github.com/rickynils/scalacheck/blob/1.12.2/src/main/scala/org/scalacheck/Prop.scala
    * TOOD: use Prop.Proof instead?
    */
    case _ : org.specs2.execute.Success => Prop.True 
    case dec : org.specs2.execute.DecoratedResult[_] => resultToPropStatus(dec.result)    
    case _ => Prop.Undecided
  } 
  
}
case class Now[T](p : T => Formula[T]) extends NextFormula[T] {
  /* Note the case class structural equality gives an implementation
   * for equals equivalent to the one below, as a Function is only
   * equal to references to the same function, which corresponds to 
   * intensional function equality. That is the only thing that makes 
   * sense if we don't have a deduction system that is able to check 
   * extensional function equality  
   *
  override def equals(other : Any) : Boolean = 
    other match {
      case that : Now[T] => that eq this
      case _ => false
    }
    *    
    */
  // FIXME: probably we cannot compute this now, because it depends
  // on each particular test case. We could modify Formula.safeWordLength
  // to return an Option that would be Some only if all these Now are 
  // constant formulas. This is not a very important feature anyway
  override def safeWordLength = ??? // Timeout(1)
  override def nextFormula = this
  override def result = None
  // override def consume(atoms : T) = Solved(p(atoms)) FIXME remove
  override def consume(atoms : T) = {
    val phiConsumed = p(atoms)
    phiConsumed.nextFormula 
  }
}
case class Not[T](phi : Formula[T]) extends Formula[T] {
  override def safeWordLength = phi safeWordLength
  override def nextFormula = new NextNot(phi.nextFormula)
}
class NextNot[T](phi : NextFormula[T]) extends Not[T](phi) with NextFormula[T] {
  override def result = None
  /** Note in the implementation of or we add Exception to the truth lattice, 
  * which always absorbs other values to signal a test evaluation error
  * */
  override def consume(atoms : T) = {
    val phiConsumed = phi.consume(atoms)   
    phiConsumed.result match {
      case Some(res) => 
        Solved (res match {
          /* Prop.True is the same as passed, see lazy val passed 
             * at https://github.com/rickynils/scalacheck/blob/1.12.2/src/main/scala/org/scalacheck/Prop.scala
             * TODO: use Prop.Proof instead?
             */
          case Prop.True => Prop.False
          case Prop.Proof => Prop.False
          case Prop.False => Prop.True 
          case Prop.Exception(_) => res 
          case Prop.Undecided => Prop.Undecided
        })
      case None => new NextNot(phiConsumed)
    }
  }
}

case class Or[T](phis : Formula[T]*) extends Formula[T] {
  override def safeWordLength = phis.map(_.safeWordLength).maxBy(_.instants)
  override def nextFormula = NextOr(phis.map(_.nextFormula):_*)
}
object NextOr {
  def apply[T](phis : NextFormula[T]*) : NextFormula[T] = if (phis.length == 1) phis(0) else new NextOr(phis:_*)
  /** @return the result of computing the or of s1 and s2 in 
   *  the lattice of truth values, adding Exception which always
   *  absorbs other values to signal a test evaluation error
   */
  private def call(s1 : Prop.Status, s2 : Prop.Status) : Prop.Status = 
    (s1, s2) match {
      case (Prop.Exception(_), _) => s1
      case (_, Prop.Exception(_)) => s2
      case (Prop.True, _) => Prop.True
      case (Prop.Proof, _) => Prop.Proof
      case (Prop.Undecided, Prop.False) => Prop.Undecided
      case _ => s2
    }  
}
class NextOr[T](phis : NextFormula[T]*) extends Or(phis:_*) with NextFormula[T] {
  override def result = None
  override def consume(atoms : T) = {
    val (phisDefined, phisUndefined) = phis.par //view
      .map { _.consume(atoms) }
      .partition { _.result.isDefined }            
    val definedStatus = (! phisDefined.isEmpty) option {
      phisDefined
      .map { _.result.get }
      .reduce { NextOr.call(_, _) }
      }     
    // short-circuit or if possible. Note an edge case when all the phis
    // are defined after consuming the input, but we might still not have a
    // positive (true of proof) result         
    if (definedStatus.isDefined && definedStatus.get.isInstanceOf[Prop.Exception])
      Solved(definedStatus.get)
    else if ((definedStatus.isDefined && (definedStatus.get == Prop.True || definedStatus.get == Prop.Proof))
             || phisUndefined.size == 0) {
      Solved(definedStatus.getOrElse(Prop.Undecided))
    } else {
      // if definedStatus is undecided keep it in case 
      // the rest of the or is reduced to false later on
      val newPhis = definedStatus match {
        case Some(Prop.Undecided) => Solved[T](Prop.Undecided) +: phisUndefined.seq //.force
        case _ => phisUndefined.seq//.force
      } 
      NextOr(newPhis :_*)
    }
  }
} 

case class And[T](phis : Formula[T]*) extends Formula[T] {
  override def safeWordLength = phis.map(_.safeWordLength).maxBy(_.instants)
  override def nextFormula = NextAnd(phis.map(_.nextFormula):_*)
}
object NextAnd {
  def apply[T](phis : NextFormula[T]*) = if (phis.length == 1) phis(0) else new NextAnd(phis:_*)
  /** @return the result of computing the and of s1 and s2 in 
   *  the lattice of truth values
   */
  private def call(s1 : Prop.Status, s2 : Prop.Status) : Prop.Status =
    (s1, s2) match {
      case (Prop.Exception(_), _) => s1
      case (_, Prop.Exception(_)) => s2
      case (Prop.False, _) => Prop.False
      case (Prop.Undecided, Prop.False) => Prop.False
      case (Prop.Undecided, _) => Prop.Undecided
      case (Prop.True, _) => s2
      case (Prop.Proof, _) => s2
    }
}
/* TODO: some refactoring could be performed so NextAnd and NextOr inherit from a 
 * common base class, due to very similar definitions of consume() and even of
 * call() in their corresponding companions
 * */
class NextAnd[T](phis : NextFormula[T]*) extends And(phis:_*) with NextFormula[T] {
  override def result = None
  override def consume(atoms : T) = {
    val (phisDefined, phisUndefined) = phis.par //.view
      .map { _.consume(atoms) }
      .partition { _.result.isDefined }     
    val definedStatus = (! phisDefined.isEmpty) option {
      phisDefined
      .map { _.result.get }
      .reduce { NextAnd.call(_, _) }
      }  
    // short-circuit and if possible. Note an edge case when all the phis
    // are defined after consuming the input, but we might still not have a
    // positive (true of proof) result
    if (definedStatus.isDefined && definedStatus.get.isInstanceOf[Prop.Exception])
      Solved(definedStatus.get)
    else if ((definedStatus.isDefined && definedStatus.get == Prop.False) 
             || phisUndefined.size == 0) {
      Solved(definedStatus.getOrElse(Prop.Undecided))
    } else {
      // if definedStatus is undecided keep it in case 
      // the rest of the and is reduced to true later on
      val newPhis = definedStatus match {
        case Some(Prop.Undecided) => Solved[T](Prop.Undecided) +: phisUndefined.seq //.force
        case _ => phisUndefined.seq ///.force
      }
      NextAnd(newPhis :_*)
    }
  }
}
case class Implies[T](phi1 : Formula[T], phi2 : Formula[T]) extends Formula[T] {
  override def safeWordLength = phi1.safeWordLength max phi2.safeWordLength
  override def nextFormula = NextOr(new NextNot(phi1.nextFormula), phi2.nextFormula)
}
object Next {
  /** @return a formula corresponding to n applications of 
   *  next on phi
   */
  def apply[T](n : Int)(phi : Formula[T]) : Formula[T] = 
    (1 to n).foldLeft(phi) { (f, _) => new Next[T](f) }
}
case class Next[T](phi : Formula[T]) extends Formula[T] {
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength + 1
  override def nextFormula = NextNext(phi.nextFormula)
}
object NextNext {
  def apply[T](phi : NextFormula[T]) : NextFormula[T] = new NextNext(phi)
  /** @return a next formula corresponding to n applications of 
   *  next on phi
   */
  def apply[T](n : Int)(phi : NextFormula[T]) : NextFormula[T] =
    (1 to n).foldLeft(phi) { (f, _) => new NextNext(f) }
}
class NextNext[T](phi : NextFormula[T]) extends Next[T](phi) with NextFormula[T] {
  override def result = None
  override def consume(atoms : T) = phi
}

case class Eventually[T](phi : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength + t - 1
  
  override def nextFormula = 
    NextOr(Seq.iterate(phi.nextFormula, t.instants) { NextNext(_) }:_*)
}
case class Always[T](phi : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength + t - 1
  override def nextFormula =
      NextAnd(Seq.iterate(phi.nextFormula, t.instants) { NextNext(_) }:_*)
}
case class Until[T](phi1 : Formula[T], phi2 : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi1.safeWordLength max phi2.safeWordLength + t - 1
  override def nextFormula = {
    val phi1Nexts = Seq.iterate(phi1.nextFormula, t.instants - 1) { NextNext(_) } 
    val phi2Nexts = Seq.iterate(phi2.nextFormula, t.instants) { NextNext(_) }
    NextOr { 
      (0 until t.instants).map { i =>
        NextAnd { 
          (0 until i).map { j => 
            phi1Nexts(j) 
          } :+ phi2Nexts(i) 
        :_* } 
      } 
    :_* }
  }
}
case class Release[T](phi1 : Formula[T], phi2 : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi1.safeWordLength max phi2.safeWordLength + t - 1
  override def nextFormula = {
    val phi1Nexts = Seq.iterate(phi1.nextFormula, t.instants) { NextNext(_) } 
    val phi2Nexts = Seq.iterate(phi2.nextFormula, t.instants) { NextNext(_) }    
    NextOr { 
      NextAnd (phi2Nexts:_*) +:
      (0 until t.instants).map { i =>
        NextAnd { 
          (0 until i).map { j => 
            phi2Nexts(j) 
          } ++ List(phi1Nexts(i), phi2Nexts(i)) 
        :_* } 
      } 
    :_* }
  }
}
 
/** Case class wrapping the number of instants / turns a temporal
 *  operator has to be resolved (e.g. an the formula in an 
 *  eventually happening).
 *   - A timeout of 1 means the operator has to be resolved now.
 *   - A timeout of 0 means the operator fails to be resolved  
 *  
 *  A type different to Int allows us to be more specified when requiring
 *  implicit parameters in functions
 * */
case class Timeout(val instants : Int) extends Serializable { 
  def +[T <% Timeout](t : T) = Timeout { instants + t.instants }
  def -[T <% Timeout](t : T) = Timeout { instants -  t.instants }
  def max[T <% Timeout](t : T) = Timeout { math.max(instants, t.instants) }
}

/** This class is used in the builder methods in Formula and companion, 
 *  to express formulas with a timeout operator at root that is missing 
 *  its timeout, wich can be provided to complete the formula with the on() method  
 */
class TimeoutMissingFormula[T](val toFormula : Timeout => Formula[T]) 
  extends Serializable {
  
  def on(t : Timeout) = toFormula(t)
  /** Alias of on that can be used for obtaining a more readable spec, for
   *  example combined with Formula.always()
   */
  def during(t : Timeout) = on(t)
}
