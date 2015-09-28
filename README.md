# sscheck
Utilities for using ScalaCheck with Spark and Spark Streaming, based on Specs2

[![Build Status](https://travis-ci.org/juanrh/sscheck.svg?branch=master)](https://travis-ci.org/juanrh/sscheck)

Use linear temporal logic to write ScalaCheck properties for Spark Streaming programs, like this one:
```scala
def checkExtractBannedUsersList(testSubject : DStream[(UserId, Boolean)] => DStream[UserId]) = {
    val batchSize = 20 
    val (headTimeout, tailTimeout, nestedTimeout) = (10, 10, 5) 
    val (badId, ids) = (15L, Gen.choose(1L, 50L))   
    val goodBatch = BatchGen.ofN(batchSize, ids.map((_, true)))
    val badBatch = goodBatch + BatchGen.ofN(1, (badId, false))
    val gen = BatchGen.until(goodBatch, badBatch, headTimeout) ++ 
               BatchGen.always(Gen.oneOf(goodBatch, badBatch), tailTimeout)
    
    type U = (RDD[(UserId, Boolean)], RDD[UserId])
    val (inBatch, outBatch) = ((_ : U)._1, (_ : U)._2)
    
    val formula : Formula[U] = {
      val badInput : Formula[U] = at(inBatch)(_ should existsRecord(_ == (badId, false)))
      val allGoodInputs : Formula[U] = at(inBatch)(_ should foreachRecord(_._2 == true))
      val badIdBanned : Formula[U] = at(outBatch)(_ should existsRecord(_ == badId))
      
      ( allGoodInputs until badIdBanned on headTimeout ) and
      ( always { badInput ==> (always(badIdBanned) during nestedTimeout) } during tailTimeout )  
    }  
    
    DStreamProp.forAll(gen)(testSubject)(formula)
  }
```

See the [**Quickstart**](https://github.com/juanrh/sscheck/wiki/Quickstart) for more details on the temporal logic, and for using this library with Spark core. 

# Acknowledgements
This work has been partially supported by the project [N-Greens Software-CM](http://n-greens-cm.org/) (S2013/ICE-2731), financed by the regional goverment of Madrid, Spain. 

Some parts of this code are based on [Spark Testing Base](https://github.com/holdenk/spark-testing-base) by Holden Karau
