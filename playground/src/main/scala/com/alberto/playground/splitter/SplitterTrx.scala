package com.alberto.playground.splitter

import com.alberto.playground.ElementField
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{OutputReceiver, ProcessElement}

object SplitterTrx extends DoFn[String, KeyVal] {
  @ProcessElement
  def apply(@ElementField word: String, out: OutputReceiver[KeyVal]): Unit = {
    println(s"Got Called with $word")
    val parts = word.split(' ')
    out.output(new KeyVal(parts(0), parts(1).toInt))
  }
}
