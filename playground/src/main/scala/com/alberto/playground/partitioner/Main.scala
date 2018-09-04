package com.alberto.playground.partitioner

import com.alberto.playground.toStdout
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Partition

object Main {
  def main(args: Array[String]): Unit = {
    println("Starting")
    val options = PipelineOptionsFactory.fromArgs(args: _*).withValidation().create()
    val pipeline = Pipeline.create(options)
    val input = pipeline.apply("FileInput" ,TextIO.read().from("file:///home/alberto/spike/beam/playground/src/main/resources/partitioner.data"))

    val partList = input.apply(Partition.of(3, new Partition.PartitionFn[String] {
      override def partitionFor(elem: String, numPartitions: Int): Int = {
        Math.abs(elem.hashCode) % numPartitions
      }
    }))

    for (i <-0 to 2)
      partList.get(i).apply(TextIO.write().withoutSharding().to(s"/tmp/part-$i.data"))

    pipeline.run().waitUntilFinish()

    for (i <- 0 to 2)
      toStdout(s"/tmp/part-$i.data")
  }
}