package es.ucm.fdi.sscheck.prop.tl

import org.specs2.execute.Result
import org.scalacheck.Prop

import scala.collection.parallel.ParSeq

import scalaz.syntax.std.boolean._
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.std.option._
      
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
  /** @return a formula where the result of applying letterToResult to the
    *         current letter must hold now
   */
  implicit def resultFunToNow[T, R <% Result](letterToResult : T => R): BindNext[T] =
    now(letterToResult andThen implicitly[Function[R, Result]])

  /** @return a formula where the result of applying letterToStatus to the
    *         current letter must hold now
    */
  implicit def statusFunToNow[T](letterToStatus: T => Prop.Status): BindNext[T] =
    nowS(letterToStatus)

  /** More succinct notation for BindNext formulas
   */
  implicit def atomsConsumerToBindNext[T](atomsConsumer: T => Formula[T]): BindNext[T] =
    BindNext.fromAtomsConsumer(atomsConsumer)

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

  /** @return a formula where the result of applying to the current tletter
    *         the projection proj and then assertion must hold now
    */
  def at[T, A, R <% Result](proj : (T) => A)(assertion : A => R): Formula[T] =
    now(proj andThen assertion andThen implicitly[Function[R, Result]])

  /** @return a formula where the result of applying to the current tletter
    *         the projection proj and then assertion must hold now
    */
  def atS[T, A](proj : (T) => A)(assertion: A => Prop.Status): Formula[T] =
    nowS(assertion compose proj)

  /** @return a formula where the result of applying to the current tletter
    *         the projection proj and then assertion must hold in the next
    *         instant
    */
  def atF[T, A](proj : (T) => A)(atomsConsumer : A => Formula[T]): Formula[T] =
    next(atomsConsumer compose proj)

  // Factories for non temporal connectives: note these act as clever constructors
  // for Or and And
  def or[T](phis: Formula[T]*): Formula[T] =
    if (phis.length == 0) Solved(Prop.False)
    else if (phis.length == 1) phis(0)
    else Or(phis:_*)
  def and[T](phis: Formula[T]*): Formula[T] =
    if (phis.length == 0) Solved(Prop.True)
    else if (phis.length == 1) phis(0)
    else And(phis:_*)

  // Factories for temporal connectives
  def next[T](phi: Formula[T]): Formula[T] = Next(phi)
  /** @return an application of BindNext to letterToFormula: the result of applying
   *         letterToFormula to the current letter must hold in the next instant
   * */
  def next[T](letterToFormula: T => Formula[T]): Formula[T] = BindNext.fromAtomsConsumer(letterToFormula)
  def nextF[T](letterToFormula: T => Formula[T]): Formula[T] = next(letterToFormula)
  /** @return the result of applying next to phi the number of
   *  times specified
   * */
  def next[T](times: Int)(phi: Formula[T]): Formula[T] = {
    require(times >= 0, s"times should be >=0, found $times")
    (1 to times).foldLeft(phi) { (f, _) => new Next[T](f) }
  }

  /* NOTE the Scaladoc description for the variants of now() are true without a
next because Result corresponds to a timeless formula, and because NextFormula.consume()
leaves the formula in a solved state without a need to consume any additional letter
after the first one 
 */
  /** @return a formula where the result of applying letterToResult to the
    *         current letter must hold now
    */
  def now[T](letterToResult: T => Result): BindNext[T] = BindNext(letterToResult)
  def nowTime[T](assertion: (T, Time) => Result): BindNext[T] = BindNext(assertion)

  /** @return a formula where the result of applying letterToStatus to the
    *         current letter must hold now
    */
  def nowS[T](letterToStatus: T => Prop.Status): BindNext[T] = BindNext.fromStatusFun(letterToStatus)
  def nowTimeS[T](assertion: (T, Time) => Prop.Status): BindNext[T] = BindNext.fromStatusTimeFun(assertion)


  def eventually[T](phi: Formula[T]): TimeoutMissingFormula[T] =
    new TimeoutMissingFormula[T](Eventually(phi, _))
  /** @return a formula where eventually the result of applying letterToFormula to the
    *         current letter must hold in the next instant
    */
  def eventually[T](letterToFormula: T => Formula[T]): TimeoutMissingFormula[T] =
    eventuallyF(letterToFormula)
  /** @return a formula where eventually the result of applying letterToResult to the
    *         current letter must hold in the next instant
    */
  def eventuallyR[T](letterToResult: T => Result): TimeoutMissingFormula[T] =
    eventually(BindNext(letterToResult))

  /** @return a formula where eventually the result of applying letterToStatus to the
    *         current letter must hold in the next instant
    */
  def eventuallyS[T](letterToStatus: T => Prop.Status): TimeoutMissingFormula[T] =
    eventually(BindNext.fromStatusFun(letterToStatus))
  /** @return a formula where eventually the result of applying letterToFormula to the
    *         current letter must hold in the next instant
    */
  def eventuallyF[T](letterToFormula: T => Formula[T]): TimeoutMissingFormula[T] =
    eventually(BindNext.fromAtomsConsumer(letterToFormula))

  /** Alias of eventually that can be used when there is a name class, for example
   *  with EventuallyMatchers.eventually
   * */
  def later[T](phi: Formula[T]) = eventually(phi)
  def later[T](assertion: T => Formula[T]) = eventually(assertion)
  def laterR[T](assertion: T => Result) = eventuallyR(assertion)
  def laterS[T](assertion: T => Prop.Status) = eventuallyS(assertion)
  def laterF[T](assertion: T => Formula[T]) = eventuallyF(assertion)

  def always[T](phi: Formula[T]) = new TimeoutMissingFormula[T](Always(phi, _))
  /** @return a formula where always the result of applying letterToFormula to the
    *         current letter must hold in the next instant
    */
  def always[T](letterToFormula: T => Formula[T]): TimeoutMissingFormula[T] =
    alwaysF[T](letterToFormula)
  /** @return a formula where always the result of applying letterToResult to the
    *         current letter must hold in the next instant
    */
  def alwaysR[T](letterToResult: T => Result): TimeoutMissingFormula[T] =
    always(BindNext(letterToResult))
  /** @return a formula where always the result of applying letterToStatus to the
    *         current letter must hold in the next instant
    */
  def alwaysS[T](letterToStatus: T => Prop.Status): TimeoutMissingFormula[T] =
    always(BindNext.fromStatusFun(letterToStatus))
  /** @return a formula where always the result of applying letterToFormula to the
    *         current letter must hold in the next instant
    */
  def alwaysF[T](letterToFormula: T => Formula[T]): TimeoutMissingFormula[T] =
    always(BindNext.fromAtomsConsumer(letterToFormula))
}

// using trait for the root of the AGT as recommended in http://twitter.github.io/effectivescala/
sealed trait Formula[T]
  extends Serializable {

  def safeWordLength: Option[Timeout]
  def nextFormula: NextFormula[T]

  // non temporal builder methods
  def unary_! = Not(this)
  def or(phi2 : Formula[T]) = Or(this, phi2)
  def and(phi2 : Formula[T]) = And(this, phi2)
  def ==>(phi2 : Formula[T]) = Implies(this, phi2)

  // temporal builder methods: next, eventually and always are methods of the Formula companion object
  def until(phi2 : Formula[T]): TimeoutMissingFormula[T] = new TimeoutMissingFormula[T](Until(this, phi2, _))
  /** @return a formula where this formula happens until the result
    *          of applying letterToFormula to the current letter holds
    *          in the next instant
    */
  def until(letterToFormula : T => Formula[T]): TimeoutMissingFormula[T] = this.untilF(letterToFormula)
  /** @return a formula where this formula happens until the result
    *          of applying letterToResult to the current letter holds
    *          in the next instant
    */
  def untilR(letterToResult : T => Result): TimeoutMissingFormula[T] = this.until(BindNext(letterToResult))
  /** @return a formula where this formula happens until the result
    *          of applying letterToStatus to the current letter holds
    *          in the next instant
    */
  def untilS(letterToStatus : T => Prop.Status): TimeoutMissingFormula[T] =
    this.until(BindNext.fromStatusFun(letterToStatus))
  /** @return a formula where this formula happens until the result
    *          of applying letterToFormula to the current letter holds
    *          in the next instant
    */
  def untilF(letterToFormula : T => Formula[T]): TimeoutMissingFormula[T] =
    this.until(BindNext.fromAtomsConsumer(letterToFormula))

  def release(phi2 : Formula[T]): TimeoutMissingFormula[T] = new TimeoutMissingFormula[T](Release(this, phi2, _))
  /** @return a formula where this formula releases the result
    *          of applying letterToFormula to the current letter from
    *          holding in the next instant
    */
  def release(letterToFormula : T => Formula[T]): TimeoutMissingFormula[T] = this.releaseF(letterToFormula)
  /** @return a formula where this formula releases the result
    *          of applying letterToResult to the current letter from
    *          holding in the next instant
    */
  def releaseR(letterToResult : T => Result): TimeoutMissingFormula[T] = this.release(BindNext(letterToResult))
  /** @return a formula where this formula releases the result
    *          of applying letterToStatus to the current letter from
    *          holding in the next instant
    */
  def releaseS(letterToStatus : T => Prop.Status): TimeoutMissingFormula[T] =
    this.release(BindNext.fromStatusFun(letterToStatus))
  /** @return a formula where this formula releases the result
    *          of applying letterToFormula to the current letter from
    *          holding in the next instant
    */
  def releaseF(letterToFormula : T => Formula[T]): TimeoutMissingFormula[T] =
    this.release(BindNext.fromAtomsConsumer(letterToFormula))
}

/** Restricted class of formulas that are in a form suitable for the
 *  formula evaluation procedure
 */
sealed trait NextFormula[T]
  extends Formula[T] {
  override def nextFormula = this

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
  override def safeWordLength = Some(Timeout(0))
  override def result = Some(status)
  // do no raise an exception in call to consume, because with NextOr we will
  // keep undecided prop values until the rest of the formula in unraveled
  override def consume(time: Time)(atoms : T) = this
}

/** This class adds information to the time and atom consumption functions
 *  used  in Now, to enable a partial implementation of safeWordLength
 * */
abstract class TimedAtomsConsumer[T](fun: Time => Function[T, Formula[T]])
  extends Function[Time, Function[T, Formula[T]]]{

  override def apply(time: Time): (T => Formula[T]) = fun(time)
  /** @return false iff fun always returns a Solved formula
   * */
  def returnsDynamicFormula: Boolean
}
class StaticTimedAtomsConsumer[T](fun: Time => Function[T, Formula[T]]) extends TimedAtomsConsumer(fun) {
  override def returnsDynamicFormula = false
}
class DynamicTimedAtomsConsumer[T](fun: Time => Function[T, Formula[T]]) extends TimedAtomsConsumer(fun) {
  override def returnsDynamicFormula = true
}
/** Formulas that have to be resolved now, which correspond to atomic proposition
 *  as functions from the current state of the system to Specs2 assertions.
 *  Note this also includes top / true and bottom / false as constant functions
 */
object BindNext {
  /* For now going for avoiding the type erasure problem, TODO check more sophisticated
   * solutions like http://hacking-scala.org/post/73854628325/advanced-type-constraints-with-type-classes
   * or http://hacking-scala.org/post/73854628325/advanced-type-constraints-with-type-classes based or
   * using ClassTag. Also why is there no conflict with the companion apply?
   */
  def fromAtomsConsumer[T](atomsConsumer: T => Formula[T]): BindNext[T] =
    new BindNext(new DynamicTimedAtomsConsumer(Function.const(atomsConsumer)))
  def fromAtomsTimeConsumer[T](atomsTimeConsumer: (T, Time) => Formula[T]): BindNext[T] =
    new BindNext(new DynamicTimedAtomsConsumer(time => atoms => atomsTimeConsumer(atoms, time)))
  def fromStatusFun[T](atomsToStatus: T => Prop.Status): BindNext[T] =
    new BindNext(new StaticTimedAtomsConsumer[T](Function.const(atomsToStatus andThen Solved.ofStatus _)))
  def fromStatusTimeFun[T](atomsTimeToStatus: (T, Time) => Prop.Status): BindNext[T] =
    new BindNext(new StaticTimedAtomsConsumer(time => atoms => Solved.ofStatus(atomsTimeToStatus(atoms, time))))
  def apply[T, R <% Result](atomsToResult: T => R): BindNext[T] =
    new BindNext(new StaticTimedAtomsConsumer[T](
      Function.const(atomsToResult andThen implicitly[Function[R,Result]] andThen Solved.ofResult _)))
  def apply[T, R <% Result](atomsTimeToResult: (T, Time) => R): BindNext[T] =
    new BindNext(new StaticTimedAtomsConsumer(time => atoms => Solved.ofResult(atomsTimeToResult(atoms, time))))
}

case class BindNext[T](timedAtomsConsumer: TimedAtomsConsumer[T])
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
      case that : BindNext[T] => that eq this
      case _ => false
    }
    *    
    */
  // we cannot fully compute this statically, because the returned
  // formula depends on the input word, but TimedAtomsConsumer.returnsDynamicFormula
  // allows us to build a safe approximation 
  override def safeWordLength = 
    (!timedAtomsConsumer.returnsDynamicFormula) option Timeout(1)
  override def result = None
  override def consume(time: Time)(atoms: T) = 
    timedAtomsConsumer(time)(atoms).nextFormula 
}
case class Not[T](phi : Formula[T]) extends Formula[T] {
  override def safeWordLength = phi safeWordLength
  override def nextFormula: NextFormula[T] = new NextNot(phi.nextFormula)
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
  override def safeWordLength = 
    phis.map(_.safeWordLength)
        .toList.sequence
        .map(_.maxBy(_.instants))
  
  override def nextFormula = NextOr(phis.map(_.nextFormula):_*)
}
object NextOr {
  def apply[T](phis: NextFormula[T]*): NextFormula[T] = if (phis.length == 1) phis(0) else new NextOr(phis:_*)
}
class NextOr[T](phis: ParSeq[NextFormula[T]]) extends NextBinaryOp[T](phis) {
  def this(seqPhis: NextFormula[T]*) {
    this(NextBinaryOp.parArgs[T](seqPhis:_*))
  }
  /** @return the result of computing the or of s1 and s2 in 
   *  the lattice of truth values, adding Exception which always
   *  absorbs other values to signal a test evaluation error
   */
  override def apply(s1: Prop.Status, s2: Prop.Status): Prop.Status = 
    (s1, s2) match {
      case (Prop.Exception(_), _) => s1
      case (_, Prop.Exception(_)) => s2
      case (Prop.True, _) => Prop.True
      case (Prop.Proof, _) => Prop.Proof
      case (Prop.Undecided, Prop.False) => Prop.Undecided
      case _ => s2
    }  
  override protected def build(phis: ParSeq[NextFormula[T]]) = 
    new NextOr(phis)
  override protected def isSolverStatus(status: Prop.Status) = 
    (status == Prop.True) || (status == Prop.Proof)
}

case class And[T](phis : Formula[T]*) extends Formula[T] {
  override def safeWordLength = 
    phis.map(_.safeWordLength)
        .toList.sequence
        .map(_.maxBy(_.instants))
  override def nextFormula = NextAnd(phis.map(_.nextFormula):_*)
}
object NextAnd {
  def apply[T](phis: NextFormula[T]*): NextFormula[T] = if (phis.length == 1) phis(0) else new NextAnd(phis:_*)
}
class NextAnd[T](phis: ParSeq[NextFormula[T]]) extends NextBinaryOp[T](phis) {
  def this(seqPhis: NextFormula[T]*) {
    this(NextBinaryOp.parArgs[T](seqPhis:_*))
  }  
  /** @return the result of computing the and of s1 and s2 in 
   *  the lattice of truth values
   */
  override def apply(s1: Prop.Status, s2: Prop.Status): Prop.Status =
    (s1, s2) match {
      case (Prop.Exception(_), _) => s1
      case (_, Prop.Exception(_)) => s2
      case (Prop.False, _) => Prop.False
      case (Prop.Undecided, Prop.False) => Prop.False
      case (Prop.Undecided, _) => Prop.Undecided
      case (Prop.True, _) => s2
      case (Prop.Proof, _) => s2
    }
  override protected def build(phis: ParSeq[NextFormula[T]]) = 
    new NextAnd(phis)
  override protected def isSolverStatus(status: Prop.Status) = 
    status == Prop.False
}

object NextBinaryOp {
  def parArgs[T](seqPhis: NextFormula[T]*): ParSeq[NextFormula[T]] = {
    // TODO could use seqPhis.par.tasksupport here 
    // to configure parallelization details, see 
    // http://docs.scala-lang.org/overviews/parallel-collections/configuration
    seqPhis.par
  } 
}
/** Abstract the functionality of NextAnd and NextOr, which are binary
 *  boolean operators that apply to a collection of formulas with a reduce()
 *  */
abstract class NextBinaryOp[T](phis: ParSeq[NextFormula[T]]) 
  extends Function2[Prop.Status, Prop.Status, Prop.Status] 
  with NextFormula[T] {
  
  // TODO: consider replacing by getting the companion of the concrete subclass 
  // following http://stackoverflow.com/questions/9172775/get-companion-object-of-class-by-given-generic-type-scala, a
  // or something in the line of scala.collection.generic.GenericCompanion (used e.g. in Seq.companion()),
  // and then calling apply to build  
  protected def build(phis: ParSeq[NextFormula[T]]): NextFormula[T]
  
  /* return true if status solves this operator: e.g. Prop.True
   * or  Prop.Proof resolve and or without evaluating anything else, 
   * otherwise return false */
  protected def isSolverStatus(status: Prop.Status): Boolean
  
  override def safeWordLength = 
    phis.map(_.safeWordLength)
        .toList.sequence
        .map(_.maxBy(_.instants))
  override def result = None
  override def consume(time: Time)(atoms : T) = {
    val (phisDefined, phisUndefined) = phis
      .map { _.consume(time)(atoms) }
      .partition { _.result.isDefined }     
    val definedStatus = (! phisDefined.isEmpty) option {
      phisDefined
      .map { _.result.get }
      .reduce { apply(_, _) }
      }  
    // short-circuit operator if possible. Note an edge case when all the phis
    // are defined after consuming the input, but we might still not have a
    // positive (true of proof) result
    if (definedStatus.isDefined && definedStatus.get.isInstanceOf[Prop.Exception])
      Solved(definedStatus.get)
    else if ((definedStatus.isDefined && isSolverStatus(definedStatus.get)) 
             || phisUndefined.size == 0) {
      Solved(definedStatus.getOrElse(Prop.Undecided))
    } else {
      // if definedStatus is undecided keep it in case 
      // the rest of the and is reduced to true later on
      val newPhis = definedStatus match {
        case Some(Prop.Undecided) => Solved[T](Prop.Undecided) +: phisUndefined
        case _ => phisUndefined
      }
      build(newPhis)
    }
  }
}

case class Implies[T](phi1 : Formula[T], phi2 : Formula[T]) extends Formula[T] {
  override def safeWordLength = for {
    safeLength1 <- phi1.safeWordLength
    safeLength2 <- phi2.safeWordLength 
  } yield safeLength1 max safeLength2
  
  override def nextFormula = NextOr(new NextNot(phi1.nextFormula), phi2.nextFormula)
}

case class Next[T](phi : Formula[T]) extends Formula[T] {
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength.map(_ + 1)
  override def nextFormula = NextNext(phi.nextFormula)
}
object NextNext {
  def apply[T](phi: => NextFormula[T]): NextFormula[T] = new NextNext(phi)
}
class NextNext[T](_phi: => NextFormula[T]) extends NextFormula[T] {
  import Formula.intToTimeout

  private lazy val phi = _phi  
  override def safeWordLength = phi.safeWordLength.map(_ + 1)
    
  override def result = None
  override def consume(time: Time)(atoms : T) = phi
}

case class Eventually[T](phi : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength.map(_ + t - 1)
  
  override def nextFormula = {
    val nextPhi = phi.nextFormula
    if (t.instants <= 1) nextPhi 
    // equivalent to paper formula assuming nt(C[phi]) = nt(C[nt(phi)]) 
    else NextOr(nextPhi, NextNext(Eventually(nextPhi, t-1).nextFormula))
  }
}
case class Always[T](phi : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength.map(_ + t - 1)
  override def nextFormula = {
    val nextPhi = phi.nextFormula
    if (t.instants <= 1) nextPhi 
    // equivalent to paper formula assuming nt(C[phi]) = nt(C[nt(phi)]) 
    else NextAnd(nextPhi, NextNext(Always(nextPhi, t-1).nextFormula))
  }
}
case class Until[T](phi1 : Formula[T], phi2 : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = for {
    safeLength1 <- phi1.safeWordLength
    safeLength2 <- phi2.safeWordLength 
  } yield (safeLength1 max safeLength2) + t -1 
    
  override def nextFormula = {
    val (nextPhi1, nextPhi2) = (phi1.nextFormula, phi2.nextFormula)
    if (t.instants <= 1) nextPhi2
    // equivalent to paper formula assuming nt(C[phi]) = nt(C[nt(phi)]) 
    else NextOr(nextPhi2, 
                NextAnd(nextPhi1, NextNext(Until(nextPhi1, nextPhi2, t-1).nextFormula)))      
  }
}
case class Release[T](phi1 : Formula[T], phi2 : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = for {
    safeLength1 <- phi1.safeWordLength
    safeLength2 <- phi2.safeWordLength 
  } yield (safeLength1 max safeLength2) + t -1
  
  override def nextFormula = {
    val (nextPhi1, nextPhi2) = (phi1.nextFormula, phi2.nextFormula)
    if (t.instants <= 1) NextAnd(nextPhi1, nextPhi2)
    // equivalent to paper formula assuming nt(C[phi]) = nt(C[nt(phi)]) 
    else NextOr(NextAnd(nextPhi1, nextPhi2), 
                NextAnd(nextPhi2, NextNext(Release(nextPhi1, nextPhi2, t-1).nextFormula)))      

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

/** @param millis: number of milliseconds since January 1, 1970 UTC
 * */
case class Time(millis: Long) extends Serializable
