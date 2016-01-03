# sscheck 0.0.1 - RDD generators

Shared Spark context for ScalaCheck generators based on parallelization of lists, through the integration of ScalaCheck and specs2

# sscheck 0.0.2 - Temporal logic generators and properties
First implementation of a temporal logic for testing Spark Streaming with ScalaCheck. This allows to define:

 * Generators for DStream defined by temporal logic formulas.
 * Properties for testing functions over DStream, using a ScalaCheck generator, and a propositional temporal logic formula as the assertion. DStreamProp.forAll defines a property that is universally quantified over the generated test cases.
