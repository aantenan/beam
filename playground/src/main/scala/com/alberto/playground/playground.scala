package com.alberto

import org.apache.beam.sdk.transforms.DoFn.Element

import scala.annotation.meta.field

package object playground {
  type ElementField = Element @field

  def toStdout(fileName: String): Unit = {
    println(s"OUTPUT OF FILE: $fileName")
    val source = scala.io.Source.fromFile(fileName)
    val lines = try source.getLines() mkString "\n" finally source.close()
    println(lines)
    println
  }
}
