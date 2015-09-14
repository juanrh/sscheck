package es.ucm.fdi.sscheck.prop.tl

import org.specs2.execute.{Result,AsResult}
import org.specs2.scalacheck.AsResultProp
import org.scalacheck.Prop 
import org.scalacheck.Prop.{undecided, proved, passed, exception, falsified}

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
  implicit def resultFunToNow[T, R <% Result](p : T => R) : Now[T] = Now(p)
  implicit def propFunToNow[T](p : T => Prop) : Now[T] = Now(p)  
  
  /** Builds a Now formula of type T by composing the projection proj on 
   *  T with an the assertion function p
   */
  def at[T, A, R <% Result](proj : (T) => A)(p : A => R) : Now[T] = Now(p compose proj)

  // builders for non temporal connectives: note these act as clever constructors
  // for Or and And
  def or[T](phis : Formula[T]*) = if (phis.length == 1) phis(0) else Or(phis:_*)
  def and[T](phis : Formula[T]*) = if (phis.length == 1) phis(0) else And(phis:_*)
    
  // builders for temporal connectives
  def next[T](phi : Formula[T]) = Next(phi)
  def eventually[T](phi : Formula[T]) = new TimeoutMissingFormula[T](Eventually(phi, _))
  /** Alias of eventually that can be used when there is a name class, for example 
   *  with EventuallyMatchers.eventually 
   * */
  def later[T](phi : Formula[T]) = eventually(phi)
  def always[T](phi : Formula[T]) = new TimeoutMissingFormula[T](Always(phi, _))  
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

/** Formulas that have to be resolved now, which correspond to atomic proposition
 *  as functions from the current state of the system to Specs2 assertions. 
 *  Note this also includes top / true and bottom / false as constant functions
 */
object Now {
  def apply[T, R <% Result](p : T => R) : Now[T] = fromResultFun(p)
  def fromResultFun[T, R <% Result](p : T => R) : Now[T] = new Now[T]((x : T) => { 
    AsResultProp.asResultToProp(p(x))
  })
}
case class Now[T](p : T => Prop) extends NextFormula[T] {
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
  override def safeWordLength = Timeout(1)
  override def nextFormula = this
  override def getResult = ???
  override def consume(atoms : T) = ???
}
case class Not[T](phi : Formula[T]) extends NextFormula[T] {
  override def safeWordLength = phi safeWordLength
  override def nextFormula = Not(phi.nextFormula)
  override def getResult = ???
  override def consume(atoms : T) = ???
}
case class Or[T](phis : Formula[T]*) extends NextFormula[T] {
  override def safeWordLength = phis.map(_.safeWordLength).maxBy(_.instants)
  override def nextFormula = Or(phis.map(_.nextFormula):_*)
  override def getResult = ???
  override def consume(atoms : T) = ???
}
case class And[T](phis : Formula[T]*) extends NextFormula[T] {
  override def safeWordLength = phis.map(_.safeWordLength).maxBy(_.instants)
  override def nextFormula = And(phis.map(_.nextFormula):_*)
  override def getResult = ???
  override def consume(atoms : T) = ???
}
case class Implies[T](phi1 : Formula[T], phi2 : Formula[T]) extends NextFormula[T] {
  override def safeWordLength = phi1.safeWordLength max phi2.safeWordLength
  override def nextFormula = Implies(phi1.nextFormula, phi2.nextFormula)
  override def getResult = ???
  override def consume(atoms : T) = ???
}
object Next {
  /** @return a formula corresponding to n applications of 
   *  next on phi
   */
  def apply[T](n : Int)(phi : Formula[T]) : Formula[T] = 
    (1 to n).foldLeft(phi) { (f, _) => new Next[T](f) }
  
  /** @return a next formula corresponding to n applications of 
   *  next on phi
   */
  def nNextF[T](n : Int)(phi : NextFormula[T]) : NextFormula[T] =
    (1 to n).foldLeft(phi) { (f, _) => new Next[T](f) }
}
case class Next[T](phi : Formula[T]) extends NextFormula[T] {
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength + 1
  override def nextFormula = Next(phi.nextFormula)
  override def getResult = ???
  override def consume(atoms : T) = ???
}
case class Eventually[T](phi : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength + t - 1
  
  override def nextFormula = 
    NextFormula.or(Seq.iterate(phi.nextFormula, t.instants) { Next(_) }:_*)
}
case class Always[T](phi : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength + t - 1
  override def nextFormula =
      NextFormula.and(Seq.iterate(phi.nextFormula, t.instants) { Next(_) }:_*)
}
case class Until[T](phi1 : Formula[T], phi2 : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, s"timeout must be greater or equal than 1, found ${t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi1.safeWordLength max phi2.safeWordLength + t - 1
  override def nextFormula = {
    val phi1Nexts = Seq.iterate(phi1.nextFormula, t.instants - 1) { Next(_) } 
    val phi2Nexts = Seq.iterate(phi2.nextFormula, t.instants) { Next(_) }
    NextFormula.or { 
      (0 until t.instants).map { i =>
        NextFormula.and { 
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
    val phi1Nexts = Seq.iterate(phi1.nextFormula, t.instants) { Next(_) } 
    val phi2Nexts = Seq.iterate(phi2.nextFormula, t.instants) { Next(_) }    
    NextFormula.or { 
      NextFormula.and (phi2Nexts:_*) +:
      (0 until t.instants).map { i =>
        NextFormula.and { 
          (0 until i).map { j => 
            phi2Nexts(j) 
          } ++ List(phi1Nexts(i), phi2Nexts(i)) 
        :_* } 
      } 
    :_* }
  }
}

object NextFormula {
  def or[T](phis : NextFormula[T]*) = if (phis.length == 1) phis(0) else Or(phis:_*)
  def and[T](phis : NextFormula[T]*) = if (phis.length == 1) phis(0) else And(phis:_*)
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
  def getResult : Option[Result] 
  
  /** @return a new formula resulting from progressing in the evaluation 
   *  of this formula by consuming the new values atoms for the atomic 
   *  propositions corresponding to a new instant of time. This corresponds
   *  to the notion of "letter simplification" in the paper
   */
  def consume(atoms : T) : NextFormula[T]
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



