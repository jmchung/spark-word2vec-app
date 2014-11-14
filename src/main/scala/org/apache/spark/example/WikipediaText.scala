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

import org.apache.spark.rdd.RDD
import org.atilika.kuromoji.{Token, Tokenizer}

import scala.collection.JavaConversions._

object WikipediaText {

  def extract(rdd: RDD[String]): RDD[String] = {
    rdd.map(line => line.replaceAll( """^[0-9]+\s*""", ""))
  }

  def format(rdd: RDD[String]): RDD[Seq[String]] = {
    val sc = rdd.sparkContext
    val kuromoji = new KuromojiTokenizer
    sc.broadcast(kuromoji)
    rdd.flatMap(sentences => sentences.split("。"))
        .map(sentence => kuromoji.extract(sentence))
  }
}

private[example]
class KuromojiTokenizer extends Serializable {

  def extract(sentence: String): Seq[String] = {
    val tokenArray = tokenizeWithFilter(sentence)
    tokenArray.map(_.getSurfaceForm)
  }

  def tokenize(line: String): Seq[Token] = {
    val tokenizer = Tokenizer.builder.mode(Tokenizer.Mode.NORMAL).build
    val scalaBufferTokens: collection.mutable.Buffer[Token] = tokenizer.tokenize(line)
    scalaBufferTokens.toSeq
  }

  def tokenizeWithFilter(line: String): Seq[Token] = {
    tokenize(line).filter { token => isNumeric(token) || hasReading(token)}
  }

  def isNumeric(token: Token): Boolean = {
    token.getAllFeaturesArray.apply(0) == "名詞" && token.getAllFeaturesArray.apply(1) == "数"
  }

  def isProperNoun(token: Token): Boolean = {
    token.getAllFeaturesArray.apply(0) == "名詞" && token.getAllFeaturesArray.apply(1) == "固有名詞"
  }

  def hasReading(token: Token): Boolean = {
    token.getReading match {
      case null => false
      case _ => true
    }
  }
}
