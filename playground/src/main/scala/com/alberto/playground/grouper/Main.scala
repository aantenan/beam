package com.alberto.playground.grouper

import com.alberto.playground.toStdout
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.join.{CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.TupleTag

object Main {
  def main(args: Array[String]): Unit = {
    println("Starting")
    val options = PipelineOptionsFactory.fromArgs(args: _*).withValidation().create()
    val pipeline = Pipeline.create(options)
    val emailsPcol = pipeline.apply("EmailsInput" ,TextIO.read().from("file:///home/alberto/spike/beam/playground/src/main/resources/emails.data"))
    val emailsKV = emailsPcol.apply("Emails", ParDo.of(AsStringKV))
    val phonesPcol = pipeline.apply("PhonesInput" ,TextIO.read().from("file:///home/alberto/spike/beam/playground/src/main/resources/phones.data"))
    val phonesKV = phonesPcol.apply("Phones", ParDo.of(AsStringKV))

    val emailsTag = new TupleTag[String]()
    val phonesTag = new TupleTag[String]()

    val results =
      KeyedPCollectionTuple.of(emailsTag, emailsKV)
        .and(phonesTag, phonesKV)
        .apply(CoGroupByKey.create())

    val combined = results.apply("Combined", ParDo.of(Merger))

    combined.apply(TextIO.write().withoutSharding().to("/tmp/merger.data"))
    pipeline.run().waitUntilFinish()
    toStdout("/tmp/merger.data")
  }
}
