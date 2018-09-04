package com.alberto.playground.flattener

import com.alberto.playground.toStdout
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.values.PCollectionList

object Main {
  def main(args: Array[String]): Unit = {
    println("Starting")
    val options = PipelineOptionsFactory.fromArgs(args: _*).withValidation().create()
    val pipeline = Pipeline.create(options)
    val combiner = pipeline.apply("FileInput" ,TextIO.read().from("file:///home/alberto/spike/beam/playground/src/main/resources/combiner.data"))
    val emails = pipeline.apply("FileInput" ,TextIO.read().from("file:///home/alberto/spike/beam/playground/src/main/resources/emails.data"))
    val phones = pipeline.apply("FileInput" ,TextIO.read().from("file:///home/alberto/spike/beam/playground/src/main/resources/phones.data"))
    val splitter = pipeline.apply("FileInput" ,TextIO.read().from("file:///home/alberto/spike/beam/playground/src/main/resources/splitter.data"))

    val collections = PCollectionList.of(combiner).and(emails).and(phones).and(splitter)
    val flattened = collections.apply(Flatten.pCollections())

    flattened.apply(TextIO.write().withoutSharding().to("/tmp/flattened.data"))
    pipeline.run().waitUntilFinish()

    toStdout("/tmp/flattened.data")
  }
}

