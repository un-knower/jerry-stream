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
package org.apache.spark.streamdm.streams.generators

import com.github.javacliparser.IntOption
import org.apache.spark.streamdm.core._
import org.apache.spark.streamdm.core.specification._
import scala.util.Random
import org.apache.spark.streamdm.core.specification.ExampleSpecification

/**
  * Stream generator for generating data from a hyperplane.
  *
  * <p>It uses the following options:
  * <ul>
  * <li> Chunk size (<b>-k</b>)
  * <li> Slide duration in milliseconds (<b>-d</b>)
  * <li> Number of features (<b>-f</b>)
  * </ul>
  */

class HyperplaneGenerator(val chunkSize: Int
                          , val slideDuration: Int
                          , val numFeatures: Int) extends Generator {
  /**
    * returns chunk size
    */
  override def getChunkSize(): Int = {
    chunkSize
  }

  /**
    * returns slide duration
    */
  override def getslideDuration(): Int = {
    slideDuration
  }

  def init(): Unit = {}

  def getExample(): Example = {
    val inputInstance = new DenseInstance(Array.fill[Double](
      numFeatures)(5.0 * getRandomNumber))
    val noiseInstance = new DenseInstance(Array.fill[Double](
      numFeatures)(getNoise))
    new Example(inputInstance.add(noiseInstance), new DenseInstance(
      Array.fill[Double](1)(label(inputInstance))))
  }

  def getRandomNumber: Double = 2.0 * Random.nextDouble() - 1.0

  def getNoise: Double = 0.5 * Random.nextGaussian()

  val weight = new DenseInstance(Array.fill[Double](
    numFeatures)(getRandomNumber))

  val bias: Double = getRandomNumber

  def label(inputInstance: Instance): Double = {
    val sum = weight.dot(inputInstance)
    if (sum > bias) 1
    else 0
  }

  /**
    * Obtains the specification of the examples in the stream.
    *
    * @return an ExampleSpecification of the examples
    */
  override def getExampleSpecification(): ExampleSpecification = {

    //Prepare specification of class attributes
    val outputIS = new InstanceSpecification()
    val classFeature = new NominalFeatureSpecification(Array("false", "true"))
    outputIS.addFeatureSpecification(0, "class", classFeature)

    //Prepare specification of input attributes
    val inputIS = new InstanceSpecification()
    for (i <- 0 until numFeatures)
      inputIS.addFeatureSpecification(i, "NumericFeature" + i)

    new ExampleSpecification(inputIS, outputIS)
  }

}