/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.dstream

import scala.language.implicitConversions
import scala.reflect.ClassTag

import org.apache.spark.streaming._
/**
 * Copied from spark-testing-base https://github.com/holdenk/spark-testing-base/blob/fedc276a312c408e5a9adad0c0c29119f8b41c86/src/main/1.3/scala/org/apache/spark/streaming/dstream/FriendlyInputDStream.scala
 * Copied here to avoid conflicting version problems with spark-testing-base, see https://github.com/juanrh/sscheck/issues/36
 * This is a verbatim copy just renaming the class, to avoid naming conflicts
 * 
 * This is a class to provide access to zerotime information
 */
abstract class SscheckFriendlyInputDStream [T: ClassTag](@transient var ssc_ : StreamingContext)
    extends InputDStream[T](ssc_) {

  var ourZeroTime: Time = null

  override def initialize(time: Time) {
    ourZeroTime = time
    super.initialize(time)
  }
}