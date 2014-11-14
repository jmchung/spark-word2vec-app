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
package org.apache.spark.example

import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.{SparkConf, SparkContext}

object Word2VecTrainer {

  def main(args: Array[String]) {
    val master = args(0)
    val maxCPUCores = args(1)
    val source = args(2)

    // Craetes a spark context
    val name = s"Word2VecApp, Cores: ${maxCPUCores}"
    val conf = new SparkConf().setMaster(master)
        .setAppName(name)
        .set("spark.cores.max", maxCPUCores)
    val sc = new SparkContext(conf)

    // Loads a text and then formats as a Word2Vec source
    val origin = sc.textFile(source)
    val lines = WikipediaText.format(origin)
    val input = WikipediaText.extract(lines).map(seq => seq.map(token => token.surfaceForm))

    // Trains a Word2Vec model
    val model = new Word2Vec().setVectorSize(10).setSeed(42L).fit(input)
    val syms = model.findSynonyms("著作", 2)
    syms.foreach(sym => println(syms.toString))
  }
}
