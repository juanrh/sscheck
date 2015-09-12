package es.ucm.fdi.sscheck.prop.tl

import org.specs2.execute.{Result,StandardResults}
import scalaz._

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
  implicit def resultFunToNow[T, R <% Result](p : T => R) : Now[T, R] = Now(p)
  
  /** Builds a Now formula of type T by composing the projection proj on 
   *  T with an the assertion function p
   */
  def at[T, A, R <% Result](proj : (T) => A)(p : A => R) : Now[T, R] =
    Now[T, R](p compose proj) 
    
  def next[T](phi : Formula[T]) = Next(phi)
  def eventually[T](phi : Formula[T]) = new TimeoutMissingFormula[T](Eventually(phi, _))
  /** Alias of eventually that can be used when there is a name class, for example 
   *  with EventuallyMatchers.eventually 
   * */
  def later[T](phi : Formula[T]) = eventually(phi)
  def always[T](phi : Formula[T]) = new TimeoutMissingFormula[T](Always(phi, _))  
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

// using trait for the root of the AGT as recommended in http://twitter.github.io/effectivescala/
sealed trait Formula[T] 
  extends Serializable {
  
  def safeWordLength : Timeout
  // FIXME: move the nextFormula design to an issue and implement 
  // the definition in the paper directly, use Function chain Seq.fill(3)((x : Int) => x + 1)
  // style to implement the iterations
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
case class Now[T, R <% Result](p : T => R) extends NextFormula[T] {
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
case class Or[T](phi1 : Formula[T], phi2 : Formula[T]) extends NextFormula[T] {
  override def safeWordLength = phi1.safeWordLength max phi2.safeWordLength
  override def nextFormula = Or(phi1.nextFormula, phi2.nextFormula)
  override def getResult = ???
  override def consume(atoms : T) = ???
}
case class And[T](phi1 : Formula[T], phi2 : Formula[T]) extends NextFormula[T] {
  override def safeWordLength = phi1.safeWordLength max phi2.safeWordLength
  override def nextFormula = And(phi1.nextFormula, phi2.nextFormula)
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
  def apply[T](n : Int)(phi : NextFormula[T]) : NextFormula[T] = 
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
  require(t.instants >=1, "timeout must be greater or equal than 1, found {t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength + t - 1
  
  override def nextFormula =    
    (for (i <- 0 until t.instants) 
      yield Next(i)(phi.nextFormula))
      .view.reduce(Or(_, _)) 
}
case class Always[T](phi : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, "timeout must be greater or equal than 1, found {t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi.safeWordLength + t - 1
  override def nextFormula =   
    (for (i <- 0 until t.instants) 
      yield Next(i)(phi.nextFormula))
      .view.reduce(And(_, _))
}
case class Until[T](phi1 : Formula[T], phi2 : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, "timeout must be greater or equal than 1, found {t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi1.safeWordLength max phi2.safeWordLength + t - 1
  override def nextFormula = {
    /* read definition with p, or, and, not, implies, next in the right hand side
     * as constructors for next formula. In one call the formula must unfold so
     * all the formulas that can be evaluated now are evaluated, that implies t -1
     * calls to NextOr: consider NextOr and NextAnd taking a list instead of two arguments, 
     * plus a clever constructor for two arguments <= done for NextOr*/
    ???
  }
}
case class Release[T](phi1 : Formula[T], phi2 : Formula[T], t : Timeout) extends Formula[T] {
  require(t.instants >=1, "timeout must be greater or equal than 1, found {t}")
  
  import Formula.intToTimeout
  override def safeWordLength = phi1.safeWordLength max phi2.safeWordLength + t - 1
  override def nextFormula = {
    ???
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



