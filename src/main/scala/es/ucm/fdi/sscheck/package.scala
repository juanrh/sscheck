package es.ucm.fdi {
  package object sscheck {
    type TestCaseId = Int
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
  }  
}

