package com.alberto.playground.grouper

import com.alberto.playground.ElementField
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{OutputReceiver, ProcessElement}
import org.apache.beam.sdk.transforms.join.CoGbkResult
import org.apache.beam.sdk.values.KV
import scala.collection.JavaConverters._

object Merger extends DoFn[KV[String, CoGbkResult], String] {
  @ProcessElement
  def apply(@ElementField input: KV[String, CoGbkResult], out: OutputReceiver[String]): Unit = {
    println(s"Got Called with $input")
    val name = input.getKey
    val schema = input.getValue.getSchema.getTupleTagList
    val emails = input.getValue.getAll(schema.get(0))
    val phones = input.getValue.getAll(schema.get(1))
    out.output(s"Name: $name => [${emails.asScala mkString "|" }] [${phones.asScala mkString "|"}]")
  }
}
