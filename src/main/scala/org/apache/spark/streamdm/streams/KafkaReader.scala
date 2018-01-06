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

import com.github.javacliparser.StringOption
import kafka.serializer.StringDecoder
import org.apache.spark.streamdm.core.Example
import org.apache.spark.streamdm.core.specification.{ExampleSpecification, InstanceSpecification, NominalFeatureSpecification}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Stream reader that gets instances from kafka.
  *
  * <p>It uses the following options:
  * <ul>
  * <li> kafka brokers (<b>-b</b>)
  * <li> kafka topics (<b>-p</b>)
  * <li> Instance type (<b>-t</b>), either <i>dense</i> or <i>sparse</i>
  * </ul>
  */
class KafkaReader(val brokers: String
                  , val topics: String
                  , val instance: String
                  , kafkaParams: Map[String, String]) extends StreamReader {
  /**
    * Obtains a stream of examples.
    *
    * @param ssc a Spark Streaming context
    * @return a stream of Examples
    */
  override def getExamples(ssc: StreamingContext): DStream[Example] = {

    assert(!(brokers + topics).contains("unset"), "brokers or topics should be set")

    val kafkaParamsAll: Map[String, String] = Map[String, String]("metadata.broker.list" -> brokers)
    kafkaParams.foreach(t2 => kafkaParamsAll + t2)
    val topicMap = topics.split(",").toSet

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParamsAll, topicMap)
      .map(x => Example.parse(x._2, instance, "dense"))
  }

  /**
    * Obtains the specification of the examples in the stream.
    *
    * @return an ExampleSpecification of the features
    */
  override def getExampleSpecification: ExampleSpecification = {

    //Prepare specification of class attributes
    val outputIS = new InstanceSpecification()
    val classFeature = new NominalFeatureSpecification(Array("+", "-"))
    outputIS.addFeatureSpecification(0, "class", classFeature)

    new ExampleSpecification(new InstanceSpecification(), outputIS)
  }
}
