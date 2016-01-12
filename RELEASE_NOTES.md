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