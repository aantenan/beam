package com.alberto.playground.grouper

import com.alberto.playground.ElementField
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{OutputReceiver, ProcessElement}
import org.apache.beam.sdk.values.KV

object AsStringKV extends DoFn[String, KV[String, String]] {
  @ProcessElement
  def apply(@ElementField word: String, out: OutputReceiver[KV[String, String]]): Unit = {
    println(s"Got Called with $word")
    val parts = word.split(' ')
    out.output(KV.of(parts(0), parts(1)))
  }
}
