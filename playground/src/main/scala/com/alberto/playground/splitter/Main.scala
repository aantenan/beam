package com.alberto.playground.splitter

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
    val linesPCol = pipeline.apply("FileInput" ,TextIO.read().from("file:///home/alberto/spike/beam/playground/src/main/resources/splitter.data"))
    val split = linesPCol.apply("Splitter", ParDo.of(SplitterTrx))
    val upper = split.apply(ParDo.of(UpperTrx))
    val square = split.apply(ParDo.of(SquareTrx))

    upper.apply(TextIO.write().withoutSharding().to("/tmp/upper.data"))
    square.apply(TextIO.write().withoutSharding().to("/tmp/square.data"))

    pipeline.run().waitUntilFinish()
    toStdout("/tmp/upper.data")
    toStdout("/tmp/square.data")
  }
}
