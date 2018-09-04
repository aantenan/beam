package com.alberto.playground.combiner

import java.lang
import scala.collection.JavaConverters._
import org.apache.beam.sdk.transforms.Combine.CombineFn

case class Concatenator() extends CombineFn[String, Acummulator, String] {
  override def createAccumulator(): Acummulator = Acummulator()

  override def addInput(accumulator: Acummulator, input: String): Acummulator = {
    accumulator.stringBuilder.append(s"$input\n")
    accumulator
  }

  override def mergeAccumulators(accumulators: lang.Iterable[Acummulator]): Acummulator = {
    val result = Acummulator()
    (accumulators asScala) foreach (s => result.stringBuilder.append(s.stringBuilder.toString))
    result
  }

  override def extractOutput(accumulator: Acummulator): String = accumulator.stringBuilder.toString()
}

case class Acummulator() {
  val stringBuilder: StringBuilder = new StringBuilder()
}
