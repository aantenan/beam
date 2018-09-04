package com.alberto.playground.external

import com.alberto.playground.toStdout
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.ParDo

object Main {
  def main(args: Array[String]): Unit = {
    println("Starting")
    val options = PipelineOptionsFactory.fromArgs(args: _*).withValidation().create()
    val pipeline = Pipeline.create(options)
    val input = pipeline.apply("FileInput" ,TextIO.read().from("file:///home/alberto/spike/beam/playground/src/main/resources/external.data"))

    val result = input.apply("ExternalProcess", ParDo.of(EchoScript))
    result.apply(TextIO.write().withoutSharding().to("/tmp/external.data"))

    pipeline.run().waitUntilFinish()

    toStdout("/tmp/external.data")
  }
}
