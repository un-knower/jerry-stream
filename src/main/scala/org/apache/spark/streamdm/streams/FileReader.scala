/*
 * Copyright (C) 2015 Holmes Team at HUAWEI Noah's Ark Lab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.streamdm.streams

import java.io.File
import scala.io.Source

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Time, Duration, StreamingContext}

import com.github.javacliparser.{IntOption, StringOption}

import org.apache.spark.streamdm.core._
import org.apache.spark.streamdm.core.specification._

/**
  * FileReader is used to read data from one file of full data to simulate a stream data.
  *
  * <p>It uses the following options:
  * <ul>
  * <li> Chunk size (<b>-k</b>)
  * <li> Slide duration in milliseconds (<b>-d</b>)
  * <li> Type of the instance to use, it should be "dense" or "sparse" (<b>-t</b>)
  * <li> Data File Name (<b>-f</b>)
  * <li> Data Header Format,uses weka's arff as default.(<b>-h</b>)
  * </ul>
  */

class FileReader(val chunkSize: Int
                 , val slideDurationOpt: Int
                 , val instance: String
                 , val instanceLimit: Int
                 , val fileName: String
                 , val dataHeadType: String) extends StreamReader with Logging {

  val headParser = new SpecificationParser
  var headFileName: String = _
  var isInited: Boolean = false
  var hasHeadFile: Boolean = false
  var lines: Iterator[String] = _
  var spec: ExampleSpecification = _
  var counter: Int = 0

  def init() {
    if (!isInited) {
      val file = new File(fileName)
      if (!file.exists()) {
        logError("file does not exists, input a new file name")
        sys.exit()
      }
      headFileName = fileName + "." + dataHeadType + ".head"
      val hfile: File = new File(headFileName)
      if (hfile.exists()) {
        // has a head file
        hasHeadFile = true
      }
      spec = headParser.getSpecification(
        if (hasHeadFile) headFileName else fileName, dataHeadType)
      for (index <- 0 until spec.out(0).range()) {
        logInfo(spec.out(0).asInstanceOf[NominalFeatureSpecification](index))
      }

      isInited = true
    }
  }

  /**
    * Obtains the specification of the examples in the stream.
    *
    * @return an ExampleSpecification of the features
    */
  override def getExampleSpecification(): ExampleSpecification = {
    init()
    spec
  }

  /**
    * Get one Example from file
    *
    * @return an Example
    */
  def getExampleFromFile: Example = {
    var exp: Example = null
    // start to read file from its beginning.
    if (lines == null || !lines.hasNext) {
      //get the whole file
      lines = Source.fromFile(fileName).getLines()
    }
    // if reach the end of file, will go to the head again
    if (!lines.hasNext) {
      exp
    }
    var line = lines.next()
    while (!hasHeadFile && (line == "" || line.startsWith(" ") ||
      line.startsWith("%") || line.startsWith("@"))) {
      line = lines.next()
    }
    if (!hasHeadFile) {
      if ("arff".equalsIgnoreCase(dataHeadType)) {
        exp = ExampleParser.fromArff(line, spec)
      } else {
        if ("csv".equalsIgnoreCase(dataHeadType)) {
          //for the csv format, we assume the first is the classification
          val index: Int = line.indexOf(",")
          line = line.substring(0, index) + " " + line.substring(index + 1).trim()
          exp = Example.parse(line, instance, "dense")
        }
      }
    } else {
      exp = Example.parse(line, instance, "dense")
    }
    exp
  }

  /**
    * Obtains a stream of examples.
    *
    * @param ssc a Spark Streaming context
    * @return a stream of Examples
    */
  override def getExamples(ssc: StreamingContext): DStream[Example] = {
    init()
    new InputDStream[Example](ssc) {
      override def start(): Unit = {
        logInfo("File reading gets started.")
      }

      override def stop(): Unit = {
        logInfo("Reading file stopped.")
      }

      override def compute(validTime: Time): Option[RDD[Example]] = {
        val examples: Array[Example] = Array.fill[Example](chunkSize)(getExampleFromFile)
        val examplesRDD = ssc.sparkContext.parallelize(examples)
        // stop the stream when it gets over N instances.
        counter = counter + 1
        val limit = instanceLimit / chunkSize
        if (counter > limit) {
          logInfo("Over limit instances. STOP!")
          ssc.stop(stopSparkContext = false, stopGracefully = false)
        }
        Some(examplesRDD)
      }

      override def slideDuration: Duration = {
        Duration(slideDurationOpt)
      }
    }
  }
}