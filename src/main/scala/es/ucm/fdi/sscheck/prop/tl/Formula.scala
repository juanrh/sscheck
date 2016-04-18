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
  implicit def resultFunToNow[T, R <% Result](assertion : T => R): Now[T] = Now(assertion)
  
  /** More succinct notation for Now formulas
   */
  implicit def statusFunToNow[T](assertion: T => Prop.Status): Now[T] = Now.fromStatusFun(assertion)
  
  /** More succinct notation for Now formulas
   */
  implicit def atomsConsumerToNow[T](atomsConsumer: T => Formula[T]): Now[T] = Now.fromAtomsConsumer(atomsConsumer)

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
  
  /* For overloads the criteria is: 
   * - avoid overload for Function, because Scala forbids them
   * - I haven't been able to make the techniques in http://stackoverflow.com/questions/3307427/scala-double-definition-2-methods-have-the-same-type-erasure
   * and http://stackoverflow.com/questions/17841993/double-definition-error-despite-different-parameter-types to work. Neither Scalaz type
   * unions. The main goal was defining an overload like Formula.always for and argument of type (T) => Result vs argument Formula[T] when using with 
   * formulas defined with the case syntax https://groups.google.com/forum/#!topic/scala-user/rkau5IcuH48  
   * - Shapeless could be an option to explore in the future, although it introduces a complex
   * dependency, specially for Scala 2.10 that doesn't have macros by default. Could explore this in the future
   * - When a overload is required we'll use the following strategy to avoid it:
   *   * For overloads for functions to Result, Prop.Status, and Formula, replace overloads for pattern f 
   *   for T => Result, fS for T => Prop.Status, and fF for T => Formula[T], see example for at()
   *   * For overloads for the 3 functions in the previous item, plus for Formula[T], use f for the Formula[T]
   *   and T => Formula[T], and then for fR for T => R <% Result, and fS for T => Prop.Status, and fF for T => Formula[T]. 
   *   The result version has two version for T => Formula[T] because the overload doesn't works ok for functions defined 
   *   with the case syntax (see https://groups.google.com/forum/#!topic/scala-user/rkau5IcuH48). See example for always() 
   * */
  
  /** Builds a Now formula of type T by composing the projection proj on 
   *  T with an the assertion function assertion
   */
  def at[T, A, R <% Result](proj : (T) => A)(assertion : A => R): Now[T] = 
    now(proj andThen assertion andThen implicitly[Function[R, Result]])
    
  /** Builds a Now formula of type T by composing the projection proj on 
   *  T with an the assertion function p
   */
  def atS[T, A](proj : (T) => A)(assertion: A => Prop.Status): Now[T] = nowS(assertion compose proj)
  
  /** Builds a Now formula of type T by composing the projection proj on 
   *  T with an the atom consumer function atomsConsumer
   */
  def atF[T, A](proj : (T) => A)(atomsConsumer : A => Formula[T]): Now[T] = nowF(atomsConsumer compose proj)
  
  /** Build a Now formula of type T, useful for defining a type context to define assertion
  *  with a partial function literal
  * */
  def now[T](assertion: T => Result): Now[T] = Now(assertion)

  /** Build a Now formula of type T, useful for defining a type context to define assertion
   *  with a partial function literal
   * */
  // could use the implicits converstions to Now here and elsewhere, but I think being
  // explicit is more clear 
  def nowS[T](assertion: T => Prop.Status): Now[T] = statusFunToNow(assertion)
  
  /** Build a Now formula of type T, useful for defining a type context to define assertion
   *  with a partial function literal
   * */
  def nowF[T](assertion: T => Formula[T]): Now[T] = atomsConsumerToNow(assertion)

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
  def next[T](phi : Formula[T]): Formula[T] = Next(phi)
  /** Applies next to a now built with assertion
   * */
  def next[T](assertion : T => Formula[T]): Formula[T] = nextF(assertion)
  /** Applies next to a now built with assertion
   * */
  def nextR[T](assertion : T => Result): Formula[T] = next(now(assertion))
  /** Applies next to a now built with assertion
   * */
  def nextS[T](assertion : T => Prop.Status): Formula[T] = next(nowS(assertion))
  /** Applies next to a now built with assertion
   * */
  def nextF[T](assertion : T => Formula[T]): Formula[T] = next(nowF(assertion))
  
  /** @return the result of applying next to phi the number of
   *  times specified
   * */
  def next[T](times: Int)(phi: Formula[T]): Formula[T] = {
    require(times >= 0, s"times should be >=0, found $times")    
    (1 to times).foldLeft(phi) { (f, _) => new Next[T](f) }
  }
  // cannot have overload next[T](times: Int)(phi: T => Formula[T]) due to currification
  /** Applies next times times to a now built with assertion
   * */
  def nextR[T](times: Int)(assertion: T => Result): Formula[T] = 
    next(times)(now(assertion))
  /** Applies next times times to a now built with assertion
   * */
  def nextS[T](times: Int)(assertion: T => Prop.Status): Formula[T] = 
    next(times)(nowS(assertion))
  /** Applies next times times to a now built with assertion
   * */
  def nextF[T](times: Int)(assertion: T => Formula[T]): Formula[T] = 
    next(times)(nowF(assertion))
  
  def eventually[T](phi : Formula[T]): TimeoutMissingFormula[T] = new TimeoutMissingFormula[T](Eventually(phi, _))
  /** Applies eventually to a now built with assertion
   * */
  def eventually[T](assertion : T => Formula[T]): TimeoutMissingFormula[T] = eventuallyF(assertion)
  /** Applies eventually to a now built with assertion
  * */
  def eventuallyR[T](assertion : T => Result): TimeoutMissingFormula[T] = eventually(now(assertion))
  /** Applies eventually to a now built with assertion
  * */
  def eventuallyS[T](assertion : T => Prop.Status): TimeoutMissingFormula[T] = eventually(nowS(assertion))
  /** Applies eventually to a now built with assertion
  * */
  def eventuallyF[T](assertion : T => Formula[T]): TimeoutMissingFormula[T] = eventually(nowF(assertion))
 
  /** Alias of eventually that can be used when there is a name class, for example 
   *  with EventuallyMatchers.eventually 
   * */
  def later[T](phi : Formula[T]) = eventually(phi)
  def later[T](assertion : T => Formula[T]) = eventually(assertion)
  def laterR[T](assertion : T => Result) = eventuallyR(assertion)
  def laterS[T](assertion : T => Prop.Status) = eventuallyS(assertion)
  def laterF[T](assertion : T => Formula[T]) = eventuallyF(assertion)
  
  def always[T](phi : Formula[T]) = new TimeoutMissingFormula[T](Always(phi, _))
  /** Applies always to a now built with assertion
   * */
  def always[T](assertion: T => Formula[T]): TimeoutMissingFormula[T] = alwaysF[T](assertion)
  /** Applies always to a now built with assertion
   * */
  def alwaysR[T](assertion: T => Result): TimeoutMissingFormula[T] = always(now(assertion))
  /** Applies always to a now built with assertion
   * */
  def alwaysS[T](assertion: T => Prop.Status): TimeoutMissingFormula[T] = always(nowS(assertion))
  /** Applies always to a now built with assertion
   * */
  def alwaysF[T](assertion: T => Formula[T]): TimeoutMissingFormula[T] = always(nowF(assertion))    
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
  import Formula._
  def until(phi2 : Formula[T]): TimeoutMissingFormula[T] = new TimeoutMissingFormula[T](Until(this, phi2, _))
  /** Applies until to a now built with assertion
   * */
  def until(assertion : T => Formula[T]): TimeoutMissingFormula[T] = this.untilF(assertion)
  /** Applies until to a now built with assertion
   * */
  def untilR(assertion : T => Result): TimeoutMissingFormula[T] = this.until(now(assertion))
  /** Applies until to a now built with assertion
   * */
  def untilS(assertion : T => Prop.Status): TimeoutMissingFormula[T] = this.until(nowS(assertion))
  /** Applies until to a now built with assertion
   * */
  def untilF(assertion : T => Formula[T]): TimeoutMissingFormula[T] = this.until(nowF(assertion))

  def release(phi2 : Formula[T]): TimeoutMissingFormula[T] = new TimeoutMissingFormula[T](Release(this, phi2, _))
  /** Applies release to a now built with assertion
   * */
  def release(assertion : T => Formula[T]): TimeoutMissingFormula[T] = this.releaseF(assertion)
  /** Applies release to a now built with assertion
   * */
  def releaseR(assertion : T => Result): TimeoutMissingFormula[T] = this.release(now(assertion))
  /** Applies release to a now built with assertion
   * */
  def releaseS(assertion : T => Prop.Status): TimeoutMissingFormula[T] = this.release(nowS(assertion))
  /** Applies release to a now built with assertion
   * */
  def releaseF(assertion : T => Formula[T]): TimeoutMissingFormula[T] = this.release(nowF(assertion))
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
   *  at a new instant of time time. This corresponds to the notion of "letter simplification" 
   *  in the paper
   */
  def consume(time: Time)(atoms: T): NextFormula[T]
}

/** Resolved formulas
 * */
object Solved {
  def apply[T](result: Result) = ofResult[T](result)
  // these are needed to resolve ambiguities with apply
  def ofResult[T](result: Result): Solved[T] = new Solved[T](Formula.resultToPropStatus(result))
  def ofStatus[T](status : Prop.Status): Solved[T] = Solved(status) 
}
// see https://github.com/rickynils/scalacheck/blob/1.12.2/src/main/scala/org/scalacheck/Prop.scala
case class Solved[T](status : Prop.Status) extends NextFormula[T] {
  override def safeWordLength = Timeout(0)
  override def nextFormula = this
  override def result = Some(status)
  // do no raise an exception in call to consume, because with NextOr we will
  // keep undecided prop values until the rest of the formula in unraveled
  override def consume(time: Time)(atoms : T) = this
} 

/** Formulas that have to be resolved now, which correspond to atomic proposition
 *  as functions from the current state of the system to Specs2 assertions. 
 *  Note this also includes top / true and bottom / false as constant functions
 */
object Now { 
  /* for now going for avoiding the type erasure problem, TODO check more sophisticated
   * solutions like http://hacking-scala.org/post/73854628325/advanced-type-constraints-with-type-classes 
   * or http://hacking-scala.org/post/73854628325/advanced-type-constraints-with-type-classes based or
   * using ClassTag. Also why is there no conflict with the companion apply?
   */  
  def fromAtomsConsumer[T](atomsConsumer: T => Formula[T]): Now[T] = 
    new Now[T](Function.const(atomsConsumer))
  def fromStatusFun[T](atomsToStatus : T => Prop.Status): Now[T] =
    new Now[T](Function.const(atomsToStatus andThen Solved.ofStatus _))  
  def apply[T, R <% Result](atomsToResult : T => R): Now[T] =
    new Now[T](Function.const(atomsToResult andThen implicitly[Function[R,Result]] andThen Solved.ofResult _))  
}

case class Now[T](timedAtomsConsumer: Time => T => Formula[T]) 
  extends NextFormula[T] {
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
  override def consume(time: Time)(atoms: T) = 
    timedAtomsConsumer(time)(atoms).nextFormula 
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
  override def consume(time: Time)(atoms : T) = {
    val phiConsumed = phi.consume(time)(atoms)   
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
  override def consume(time: Time)(atoms : T) = {
    val (phisDefined, phisUndefined) = phis.par //view
      .map { _.consume(time)(atoms) }
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
  override def consume(time: Time)(atoms : T) = {
    val (phisDefined, phisUndefined) = phis.par //.view
      .map { _.consume(time)(atoms) }
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

case class Next[T](phi : Formula[T]) extends Formula[T] {
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength + 1
  override def nextFormula = NextNext(phi.nextFormula)
}
object NextNext {
  def apply[T](phi : NextFormula[T]) : NextFormula[T] = new NextNext(phi)
}
class NextNext[T](phi : NextFormula[T]) extends Next[T](phi) with NextFormula[T] {
  override def result = None
  override def consume(time: Time)(atoms : T) = phi
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

/** FIXME: should be feature pairing to Spark's. Should not introduce a 
 *  dependency to Spark here
 *  
 *  @param millis: number of milliseconds since January 1, 1970 UTC
 * */
case class Time(millis: Long) extends Serializable
