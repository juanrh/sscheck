# sscheck 0.3.2
Minor maintenance release

 * replace `Now` by `BindNext`: #53 and move overloads of `Formula.now` that don't generate a result immediately to overloads of `Formula.next`, so the DSL is more clear
 * add `beEqualAsSetTo` RDD matcher
 * minor scaladoc fixes
 * rename overloads of forAllDStream for more than 1 argument to avoid having to specify the type parameters in all usages

# sscheck 0.3.1
Bug fixes

 * Ensure we don't try to `cache()` generated batches more than once, which throws a Spark exception.
 * Fix formatting of print of generated DStreams.
 * Bug fix: the state of the formula has to be updated even for empty batches, because the state and output of stateful operators might be updated anyway (e.g. `PairDStreamFunctions.reduceByKeyAndWindow`)
 * Fix base case of safeWordLength [#51](https://github.com/juanrh/sscheck/issues/51).

# sscheck 0.3.0
First order quantifiers, many DStreams in properties, and performance improvements

 * Add first order quantifiers on letters and optionally use atoms time in now, in the style of TPTL [#46](https://github.com/juanrh/sscheck/pull/46).
 * Support several DStreams in properties: 1 input 1 derived, 2 input 1 derived, 1 input 2 derived, 2 input 2 derived [#16](https://github.com/juanrh/sscheck/pull/16).
 * Lazy next form, so the system might scales to complex formulas or with long timeouts [#25](https://github.com/juanrh/sscheck/pull/25). Only Next is lazy, which is enough combined with a nested next formula generation.
 * Simplify concurrency: synchronization to wait for the Streaming Context to stop; improve usage of parallel collections.
 * Fix safeWordLength to return an Option, as it cannot be always statically computed due to first order quantifiers.
 * Update to Spark 1.6.2 to get rid of some bugs.

# sscheck 0.2.4
Added cross Scala version compatibility, 2.10 and 2.11, see [#42](https://github.com/juanrh/sscheck/pull/42)

# sscheck 0.2.3
Bug fixing and code cleanup

  * Remove dependency to spark-testing-base and multisets in order to fix [#36](https://github.com/juanrh/sscheck/issues/36)
  * Remove unused code from preliminary approaches that were later discarded
  * Update Spark to version 1.6.0

# sscheck 0.2.2
Update Spark to version 1.6.0

# sscheck 0.2.1
Bug fix implementation of temporal properties. Uses of `DStreamProp.forAll` combining with extending the trait `SharedStreamingContextBeforeAfterEach` should be replaced by extending the trait `DStreamTLProperty` and calling `forAllDStream`. This solves:

  * Execution of a test case is now independent from others, as a new streaming context is created for each test case. This is particularly important for stateful DStream transformations
  * Replaced uses `DynSingleSeqQueueInputDStream` by `TestInputStream` from [spark-testing-base](https://github.com/holdenk/spark-testing-base), which implements checkpointing correctly
  * fixed [#32](https://github.com/juanrh/sscheck/issues/32) and [#31](https://github.com/juanrh/sscheck/issues/31)

# sscheck 0.2.0 - Temporal logic generators and properties
First implementation of a temporal logic for testing Spark Streaming with ScalaCheck. This allows to define:

   * Generators for DStream defined by temporal logic formulas.
   * Properties for testing functions over DStream, using a ScalaCheck generator, and a propositional temporal logic formula as the assertion. DStreamProp.forAll defines a property that is universally quantified over the generated test cases.

# sscheck 0.1.0 - RDD generators

Shared Spark context for ScalaCheck generators based on parallelization of lists, through the integration of ScalaCheck and specs2
