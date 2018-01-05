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

package org.apache.spark.streamdm.classifiers

import org.apache.spark.streamdm.classifiers.model._
import org.apache.spark.streamdm.core._
import org.apache.spark.streamdm.core.specification.ExampleSpecification
import org.apache.spark.streaming.dstream._

/**
  * The SGDLearner trains a LinearModel using the stochastic gradient descent
  * algorithm. The type of loss function, the lambda learning
  * reate parameter, and the number of features need to be specified in the
  * associated Task configuration file.
  *
  * <p>It uses the following options:
  * <ul>
  * <li> Number of features (<b>-f</b>)
  * <li> Rate of learning parameter (<b>-l</b>)
  * <li> Loss function (<b>-o</b>), an object of type <tt>Loss</tt>
  * <li> Regularizer (<b>-r</b>), an object of type <tt>Regularizer</tt>
  * <li> Regularization parameter (<b>-p</b>)
  * </ul>
  */
class SGDLearner(val numFeatures: Int
                 , val lambda: Double
                 , lossFunction: Loss
                 , val regularizerParameter: Double
                 , val regularizer: Regularizer) extends Classifier {

  type T = LinearModel

  var model: LinearModel = _
  val loss: Loss = lossFunction
  var exampleLearnerSpecification: ExampleSpecification = _

  /* Init the model based on the algorithm implemented in the learner,
   * from the stream of instances given for training.
   *
   */
  override def init(exampleSpecification: ExampleSpecification): Unit = {
    exampleLearnerSpecification = exampleSpecification
    model = new LinearModel(loss, new DenseInstance(Array.fill[Double]
      (numFeatures + 1)(0.0)), numFeatures)
  }

  /* Train the model using stochastic gradient descent.
   *
   * @param input a stream of instances
   */
  override def train(input: DStream[Example]): Unit = {
    input.foreachRDD(rdd => {
      val chModels = rdd.aggregate(
        //initialize with the previous model
        (new LinearModel(loss, model.modelInstance, model.numFeatures), 0.0))(
        (mod, inst) => {
          //for each instance in the RDD,
          //add the gradient and the regularizer and update the model
          val grad = mod._1.gradient(inst)
          val reg = mod._1.regularize(regularizer).map(f =>
            f * regularizerParameter)
          val change = grad.add(reg).map(f => f * lambda)
          (mod._1.update(new Example(change)), 1.0)
        },
        (mod1, mod2) =>
          //add all the models together, keeping the count of the RDDs used
          (mod1._1.update(new Example(mod2._1.modelInstance)),
            mod1._2 + mod2._2)
      )
      if (chModels._2 > 0)
      //finally, take the average of the models as the new model
        model = new LinearModel(loss,
          chModels._1.modelInstance.map(f => f / chModels._2),
          model.numFeatures)
    })
  }

  /* Predict the label of the Example stream, given the current Model
   *
   * @param instance the input Example stream 
   * @return a stream of tuples containing the original instance and the
   * predicted value
   */
  override def predict(input: DStream[Example]): DStream[(Example, Double)] =
    input.map(x => (x, model.predict(x)))

  /* Gets the current LinearModel used for the SGDLearner.
   * 
   * @return the LinearModel object used for training
   */
  override def getModel: LinearModel = model
}
