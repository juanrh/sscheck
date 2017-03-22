package es.ucm.fdi.sscheck.matcher {

  package specs2 {
    import org.apache.spark.rdd.RDD
    import org.specs2.matcher.Matcher
    import org.specs2.matcher.MatchersImplicits._
    import scalaz.syntax.std.boolean._

    object RDDMatchers {
      /** Number of records to show on failing predicates
       *  */
      private val numErrors = 4

      /** @return a matcher that checks whether predicate holds for all the records of
       *  an RDD or not.
       *  
       *  NOTE: in case exceptions like the following are generated when using a closure for the
       *  the predicate, use the other variant of foreachRecord() to explicitly specify the context 
       *  available to the closure 
       *  {{{
       *  Driver stacktrace:,org.apache.spark.SparkException: Job aborted due to stage failure: 
       *  Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0, localhost): 
       *  java.io.InvalidClassException: org.specs2.execute.Success; no valid constructor 
       *  }}}
       * */
      def foreachRecord[T](predicate: T => Boolean): Matcher[RDD[T]] = { (rdd: RDD[T]) =>
        val failingRecords = rdd.filter(! predicate(_))
        (
          failingRecords.isEmpty,
          "each record fulfils the predicate",
          s"predicate failed for records ${failingRecords.take(numErrors).mkString(", ")} ..."
        )
      }
  
      def foreachRecord[T,C](predicateContext: C)(toPredicate: C => (T => Boolean)): Matcher[RDD[T]] = {
        val predicate = toPredicate(predicateContext)
        foreachRecord(predicate)
      }
      
      /** @return a matcher that checks whether predicate holds for at least one of the records of
      *  an RDD or not.
      *
      *  NOTE: in case exceptions like the following are generated when using a closure for the
      *  the predicate, use the other variant of foreachRecord() to explicitly specify the context
      *  available to the closure
      *  {{{
      *  Driver stacktrace:,org.apache.spark.SparkException: Job aborted due to stage failure:
      *  Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0, localhost):
      *  java.io.InvalidClassException: org.specs2.execute.Success; no valid constructor
      *  }}}
      * */
      def existsRecord[T](predicate: T => Boolean): Matcher[RDD[T]] = { (rdd: RDD[T]) =>
        val exampleRecords = rdd.filter(predicate(_))
        (
          ! exampleRecords.isEmpty,
          "some record fulfils the predicate",
          s"predicate failed for all the records"  
        )
      }
      
      def existsRecord[T,C](predicateContext: C)(toPredicate: C => (T => Boolean)): Matcher[RDD[T]] = {
        val predicate = toPredicate(predicateContext)
        existsRecord(predicate)
      }

      /** @return a Matcher that checks that both RDDs are equal as sets. It is is recommended to
        *         cache both RDDs to avoid recomputation
        * */
      def beEqualAsSetTo[T](actualRDD: RDD[T]): Matcher[RDD[T]] = { (expectedRDD: RDD[T]) =>
        val inActualNotInExpected = actualRDD.subtract(expectedRDD)
        val inExpectedNotInActual = expectedRDD.subtract(actualRDD)
        lazy val errorMsg: String =
          List(
            (!inActualNotInExpected.isEmpty) option
              s"unexpected records: ${inActualNotInExpected.take(numErrors).mkString(",") ...}",
            (!inExpectedNotInActual.isEmpty) option
              s"missing records:  ${inExpectedNotInActual.take(numErrors).mkString(",") ...}"
          ).filter(_.isDefined)
            .map(_.get).mkString(",")
        (
          inActualNotInExpected.isEmpty && inExpectedNotInActual.isEmpty,
          "both RDDs contain the same records",
          errorMsg
        )
      }

      // TODO: idea for predicate with partial functions rdd.collect{case record => true}.toLocalIterator.hasNext must beTrue
    }  
  }
}