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

import java.io._

import org.apache.spark.streamdm.core._
import org.apache.spark.streamdm.core.specification._
import org.apache.spark.streamdm.streams.generators._

/**
  * FileWriter use generator to generate data and save to file or HDFS for simulation or test.
  *
  * <p>It uses the following options:
  * <ul>
  * <li> Chunk number (<b>-n</b>)
  * <li> File Name (<b>-f</b>)
  * <li> Data Head Type, default is "arff" (<b>-f</b>)
  * <li> Generator (<b>-g</b>)
  * </ul>
  */

class FileWriter(val chunkNumber: Int
                 , val fileName: String
                 , val dataHeadType: String = "arff"
                 , val generator: Generator) extends Serializable {

  val headParser = new SpecificationParser


  var headType: String = "arff"

  /**
    * writes sample data to file or HDFS file
    */
  def write(): Unit = {
    headType = dataHeadType
    if (generator != null)
      write(fileName, chunkNumber)
  }

  /**
    * writes sample data to file or HDFS file
    *
    * @param fileName    file name to be stored
    * @param chunkNumber chunk number would be stored
    * @return Unit
    */
  private def write(fileName: String, chunkNumber: Int): Unit = {
    if (fileName.startsWith("HDFS")) writeToHDFS(fileName, chunkNumber)
    else writeToFile(fileName, chunkNumber)
  }

  /**
    * writes sample data to file
    *
    * @param fileName    file name to be stored
    * @param chunkNumber chunk number would be stored
    * @return Unit
    */
  private def writeToFile(fileName: String, chunkNumber: Int): Unit = {

    val headFile: File = new File(fileName + "." + headType + ".head")
    val headWriter = new PrintWriter(headFile)
    val file: File = new File(fileName)
    val writer = new PrintWriter(file)
    //   val writerArrf = new PrintWriter(fileArrf)
    try {
      //write to head file
      val head = headParser.getHead(generator.getExampleSpecification, headType)
      headWriter.write(head)
      //write to data file
      for (i <- 0 until chunkNumber) {
        //println(i)
        val examples: Array[Example] = generator.getExamples()
        val length: Int = examples.length
        var str: String = new String
        for (i <- 0 until length) {
          str = examples(i).toString()
          writer.append(str + "\n")
          writer.flush()
        }
      }
    } finally {
      headWriter.close()
      writer.close()
    }
  }

  /**
    * writes sample data to HDFS file
    *
    * @param fileName    file name to be stored
    * @param chunkNumber chunk number would be stored
    * @return Unit
    */
  private def writeToHDFS(fileName: String, chunkNumber: Int): Unit = {
    //todo
  }

}