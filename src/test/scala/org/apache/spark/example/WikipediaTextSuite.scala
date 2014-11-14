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

import org.apache.spark.util.{LocalSparkContext, SampleData}
import org.scalatest.FunSuite

class WikipediaTextSuite extends FunSuite with LocalSparkContext with SampleData {

  test("extract") {
    val filepath = getSampleDataFilePath()
    val rdd = sc.textFile(filepath)

    val text = WikipediaText.format(rdd)
    assert(text.count() === 100)
    assert(text.collect().apply(0).length === 71222)
    assert(text.collect().apply(1).length === 207)
    assert(text.collect().apply(98).length === 20662)
    assert(text.collect().apply(99).length === 7039)
  }

  test("format") {
    val filepath = getSampleDataFilePath()
    val rdd = sc.textFile(filepath)

    // check the number of sentences in the sample data
    val text = WikipediaText.format(rdd)
    val tokenizedSentences = WikipediaText.extract(text)
    assert(tokenizedSentences.count() === 10746)

    val collected = tokenizedSentences.collect()
    assert(collected(0).length === 17)
    assert(collected(1).length === 23)
  }

  test("extractNouns") {
    val filepath = getSampleDataFilePath()
    val rdd = sc.textFile(filepath)

    val text = WikipediaText.format(rdd)
    val nouns = WikipediaText.extractNoun(text)
    val collected = nouns.collect()
    assert(collected.length === 3300)
  }
}

class KuromojiTokenizerSuite extends FunSuite {

  test("tokenize010") {
    val text = "リーグ戦が採用されているＡＴＰツアー・ファイナルは大会規定で補欠２人が用意される。" +
        "原則的には年間獲得ポイントで大会出場権を得た上位８選手に次ぐ２選手が選ばれ、" +
        "規定で開幕前の公式会見への出席、１次リーグ終了まで欠場者が出た場合に備えることが義務付けられる。"

    val tokenizer = new KuromojiTokenizer
    val result = tokenizer.tokenize(text)
    assert(result.length === 78)
    assert(result.filter(_.getAllFeaturesArray.apply(0) == "名詞").length === 39)
    assert(result.filter(_.getAllFeaturesArray.apply(0) == "動詞").length === 13)
  }

  test("tokenize020") {
    val text = """{{出典の明記|date=2014年9月11日 (木) 10:37 (UTC)}}\n{{WikipediaPage|ウィキペディアにおける著作権に" +
        "ついては、[[Wikipedia:著作権]]、[[Wikipedia:著作権問題]]、[[Wikipedia:ガイドブック 著作権に注意]]を" +
        "ご覧ください。}}\n'''著作権'''（ちょさくけん）は、[[言語]]、[[音楽]]、[[絵画]]、[[建築]]、[[図形]]、" +
        "[[映画]]、[[写真]]、[[プログラム (コンピュータ)|コンピュータプログラム]]などの表現形式によって自らの'''" +
        "思想・感情を創作的に表現した[[著作物]]'''を排他的に支配する財産的な[[権利]]である。著作権は[[特許権]や" +
        "[[商標権]]にならぶ[[知的財産権]]の一つとして位置づけられている。\n\n[[著作者]]の権利には、人格的な権利" +
        "である[[著作者人格権]]と、財産的な権利である（狭義の）著作権とがある。両者を合わせて（広義の）著作権と" +
        "呼ぶ場合があるが、日本の[[著作権法]]では「著作権」という用語は狭義の財産的な権利を指して用いられており" +
        "（著作権法第17条第1項）、本項においても、狭義の意味で用いる。\n\n著作権の保護については、『[[文学的及び" +
        "美術的著作物の保護に関するベルヌ条約]]』（ベルヌ条約）、『[[万国著作権条約]]』、『[[著作権に関する" +
        "世界知的所有権機関条約]]』（WIPO著作権条約）、『[[知的所有権の貿易関連の側面に関する協定]]』（TRIPS協定）" +
        "などの条約が、保護の最低要件などを定めており、これらの条約の締約国が、条約上の要件を満たす形で、国内の" +
        "著作権保護法令を定めている。\n\n[[著作権者]]を表すコピーライトマーク「©」は、現在では、方式主義をとる" +
        "[[カンボジア]]以外では著作権の発生要件としての法的な意味はないが、著作権者をわかりやすく表すなどのために" +
        "広く使われている。\n{{Main|#コピーライトマーク}}\n\n==権利としての特徴==\n著作権は[[著作者]]に対して" +
        "付与される[[財産権]]の一種であり、著作者に対して、著作権の対象である著作物を排他的に利用する権利を認める" +
        "ものである。例えば、小説の作者は、その小説を排他的に出版、映画化、翻訳する権利を有しており、他人が著作者の" +
        "許諾なしに無断で出版、映画化、翻訳した場合には、著作権を侵害することになる。著作権は、多くの[[#支分権|" +
        "支分権]]から成り立っており、しばしば「権利の束」と呼ばれる。\n\n著作権は[[無体財産権]]であって、著作者が" +
        "作品の媒体たる有体物の[[所有権]]を他人に譲渡した場合でも、その行為によって著作権が消滅したり、移転したり" +
        "することはない。一方、無体物である著作権自体についても、その全部又は一部を譲渡することが可能である。" +
        "例えば、小説家は執筆原稿を出版者に譲渡しても、依然として著作者としての諸権利を有しているが、契約により" +
        "著作権自体を譲渡することもできる。なお、著作権は、譲渡のほかに、利用許諾によって他者に利用させることも" +
        "できる。\n\n著作権は'''相対的独占権'''あるいは排他権である。特許権や意匠権のような絶対的独占権ではない。"""

    val tokenizer = new KuromojiTokenizer
    val result = tokenizer.tokenize(text)
    assert(result.length === 888)
    assert(result.filter(_.getAllFeaturesArray.apply(0) == "名詞").length === 500)
    assert(result.filter(_.getAllFeaturesArray.apply(0) == "動詞").length === 56)

    val pronouns = result.map(_.getAllFeaturesArray.apply(0)).distinct
    assert(pronouns.length === 10)

    val first = result(0)
    assert(first.getSurfaceForm === "{{")
    assert(first.getAllFeaturesArray.apply(0) === "名詞")

    val surfaces = tokenizer.tokenizeWithFilter(text)
    val hoge = surfaces.filter(token => token.getAllFeaturesArray.apply(1) == "サ変接続")
    assert(surfaces.length === 643)
  }

  test("hasReading") {
    val text = "{{"
    val tokenizer = new KuromojiTokenizer
    val result = tokenizer.tokenize(text)
    assert(tokenizer.hasReading(result.apply(0)) === false)
  }
}
