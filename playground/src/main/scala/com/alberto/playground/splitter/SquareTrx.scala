package com.alberto.playground.splitter

import com.alberto.playground.ElementField
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{OutputReceiver, ProcessElement}

object SquareTrx extends DoFn[KeyVal, String] {
  @ProcessElement
  def apply(@ElementField input: KeyVal, out: OutputReceiver[String]): Unit = {
    println(s"SquareTrx Got Called with $input")
    out.output((input.value() * input.value()).toString)
  }
}
