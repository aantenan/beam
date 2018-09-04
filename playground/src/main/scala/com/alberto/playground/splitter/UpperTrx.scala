package com.alberto.playground.splitter

import com.alberto.playground.ElementField
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{OutputReceiver, ProcessElement}

object UpperTrx extends DoFn[KeyVal, String] {
  @ProcessElement
  def apply(@ElementField word: KeyVal, out: OutputReceiver[String]): Unit = {
    println(s"UpperTrx Got Called with $word")
    out.output(word.key.toUpperCase())
  }
}
