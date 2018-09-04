package com.alberto.playground.external

import java.io.File

import com.alberto.playground.ElementField
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{OutputReceiver, ProcessElement}

import scala.sys.process._

object EchoScript extends DoFn[String, String] {
  @ProcessElement
  def apply(@ElementField word: String, out: OutputReceiver[String]): Unit = {
    println(s"Got Called with $word")
    val fileName = s"/tmp/$word.ret"
    val command = s"/bin/echo 'This is the word I received: $word'"
    (command #> new File(fileName)).!
    val source = scala.io.Source.fromFile(fileName)
    val lines = try source.getLines() mkString "\n" finally source.close()
    out.output(lines)
  }
}