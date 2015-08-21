package es.ucm.fdi {
  package object sscheck {
    type TestCaseId = Int
    type TestCaseRecord[T] = (TestCaseId, Option[T])
  }
  
  package sscheck {
    import java.util.concurrent.atomic.AtomicInteger
    
    /** This class can be used to generate unique test case identifiers per each instance. 
    * */
    class TestCaseIdCounter {
      val counter = new AtomicInteger(0)
      /** @return a fresh TestCaseId. This method is thread safe 
       * */
      def nextId() : TestCaseId = counter.getAndIncrement() 
    }

    /** This class can be used to wrap the result of the execution of a Prop
     * @param testCaseId id of the test case that produced this result 
     * @param result result of the test
     * */
    case class PropResult(testCaseId : TestCaseId, result : org.specs2.execute.Result)
  }  
}

