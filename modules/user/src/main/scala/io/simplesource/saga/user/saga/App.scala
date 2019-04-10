package io.simplesource.saga.user.saga

import io.circe.Json
import io.simplesource.kafka.spec.WindowSpec
import io.simplesource.saga.model.specs.{ActionSpec, SagaSpec}
import io.simplesource.saga.saga.SagaApp
import io.simplesource.saga.shared.streams.StreamAppConfig
import io.simplesource.saga.scala.serdes.JsonSerdes
import io.simplesource.saga.user.constants

object App {
  def main(args: Array[String]): Unit = {
    startSagaCoordinator()
  }

  def startSagaCoordinator(): Unit = {
    val sagaSpec =
      new SagaSpec(JsonSerdes.sagaSerdes[Json], new WindowSpec(3600L))
    SagaApp
      .of[Json](sagaSpec, actionSpec)
      .withActions(constants.userActionType,
                   constants.accountActionType,
                   constants.asyncActionType,
                   constants.httpActionType)
      .run(StreamAppConfig.of("saga-coordinator-1", constants.kafkaBootstrap))
  }

  lazy val actionSpec: ActionSpec[Json] =
    ActionSpec.of[Json](JsonSerdes.actionSerdes[Json])
}
