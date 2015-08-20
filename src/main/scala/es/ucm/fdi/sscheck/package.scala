package es.ucm.fdi.sscheck

import org.specs2.execute.Result
import java.util.concurrent.atomic.AtomicInteger

object TestCaseIdCounter {
  type TestCaseId = Int
}
/** This class can be used to generate unique test case identifiers per each instance. 
 * */
class TestCaseIdCounter {
  import TestCaseIdCounter.TestCaseId
  val counter = new AtomicInteger(0)
  /** @return a fresh TestCaseId. This method is thread safe 
   * */
  def nextId() : TestCaseId = counter.getAndIncrement() 
}

/** This class can be used to wrap the result of the execution of a Prop
 * @param testCaseId id of the test case that produced this result 
 * @param result result of the test
 * */
case class PropResult(testCaseId : TestCaseIdCounter.TestCaseId, result : Result)