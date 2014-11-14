package org.apache.spark.example

import org.atilika.kuromoji.Tokenizer

object Kuromoji {

  def apply() {
    val tokenizer = Tokenizer.builder.mode(Tokenizer.Mode.NORMAL).build
  }
}
