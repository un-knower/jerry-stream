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

package org.apache.spark.streamdm.classifiers.meta

import java.util.Random

import org.apache.spark.streamdm.classifiers.Classifier
import org.apache.spark.streamdm.classifiers.model._
import org.apache.spark.streamdm.core._
import org.apache.spark.streamdm.core.specification.ExampleSpecification
import org.apache.spark.streamdm.utils.Utils
import org.apache.spark.streaming.dstream._

/**
  * The Bagging classifier trains an ensemble of classifier to improve performance.
  * It is based on doing sampling with replacement at the input of each classifier.
  *
  * <p>It uses the following options:
  * <ul>
  * <li> Base Classifier to use (<b>-l</b>)
  * <li> Size of the ensemble (<b>-s</b>)
  * </ul>
  */

class Bagging(val baseClassifier: Classifier, val ensembleSize: Int) extends Classifier {

  type T = LinearModel

  var classifiers: Array[Classifier] = _

  var exampleLearnerSpecification: ExampleSpecification = _

  val classifierRandom: Random = new Random()

  /* Init the model based on the algorithm implemented in the learner,
   * from the stream of instances given for training.
   *
   */
  override def init(exampleSpecification: ExampleSpecification): Unit = {
    exampleLearnerSpecification = exampleSpecification

    //Create the learner members of the ensemble
    val baseClassifier: Classifier = baseClassifier
    val sizeEnsemble = ensembleSize
    classifiers = new Array[Classifier](sizeEnsemble)

    for (i <- 0 until sizeEnsemble) {
      classifiers(i) = Utils.copyClassifier(baseClassifier)
      classifiers(i).init(exampleSpecification)
    }
  }

  /* Train the ensemble training each of the members of the ensemble
     *
     * @param input a stream of instances
     * @return the updated Model
     */
  override def train(input: DStream[Example]): Unit = {
    for (i <- 0 until ensembleSize) {
      classifiers(i).train(input.map(onlineSampling))
    }

    //Online Sampling with replacement
    def onlineSampling(example: Example): Example = {
      val weight = Utils.poisson(1.0, classifierRandom);
      new Example(example.in, example.out, weight * example.weight)
    }
  }

  /* Builds a stream of examples and predictions based on the algorithm implemented in the classifier,
    * from the stream of instances given for testing.
    *
    * @param input a stream of examples
    * @return a stream of examples and numeric values
    */
  override def predict(input: DStream[Example]): DStream[(Example, Double)] =
    input.map(x => (x, ensemblePredict(x)))

  /* Gets the current Model used for the Learner.
  *
  * @return the Model object used for training
  */
  override def getModel: LinearModel = null

  /* Predict the label of an example combining the predictions of the members of the ensemble
   *
   * @param example the Example which needs a class predicted
   * @return the predicted value
   */
  def ensemblePredict(example: Example): Double = {
    val sizeEnsemble = ensembleSize
    val predictions: Array[Double] = new Array(sizeEnsemble)
    for (i <- 0 until sizeEnsemble) {
      predictions(i) = classifiers(i).getModel.asInstanceOf[ClassificationModel].predict(example)
    }
    Utils.majorityVote(predictions, numberClasses())
  }

  def numberClasses(): Integer = {
    if (exampleLearnerSpecification == null) 2
    else exampleLearnerSpecification.out(0).range()
  }
}
