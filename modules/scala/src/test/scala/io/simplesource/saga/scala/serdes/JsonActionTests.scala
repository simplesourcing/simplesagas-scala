package io.simplesource.saga.scala.serdes
import java.util.{Optional, UUID}

import io.circe.Json
import io.circe.generic.auto._
import io.simplesource.api.CommandId
import io.simplesource.data.Result
import io.simplesource.saga.model.action.{ActionCommand, ActionId}
import io.simplesource.saga.model.messages.{ActionRequest, ActionResponse, UndoCommand}
import io.simplesource.saga.model.saga.{SagaError, SagaId}
import org.scalatest.{Matchers, WordSpec}

class JsonActionTests extends WordSpec with Matchers {
  import TestTypes._
  import io.circe.syntax._
  "action serdes" must {
    val serdes =
      JsonSerdes.actionSerdes[Json]
    val topic = "topic"

    "serialise and deserialise SagaId" in {
      val initial = SagaId.random()
      val ser =
        serdes.sagaId().serializer().serialize(topic, initial)
      val de =
        serdes.sagaId().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise ActionId" in {
      val initial = ActionId.random()
      val ser =
        serdes.actionId().serializer().serialize(topic, initial)
      val de =
        serdes.actionId().deserializer().deserialize(topic, ser)
      de shouldBe initial
    }

    "serialise and deserialise action requests" in {
      val request =
        ActionRequest.of(
          SagaId.random(),
          ActionId.random(),
          ActionCommand
            .of(CommandId.random(),
                (UserCommand
                  .Insert(UUID.randomUUID(), "", ""): UserCommand).asJson,
                "action"),
          false
        )

      val ser = serdes.request.serializer().serialize(topic, request)
      val de  = serdes.request.deserializer().deserialize(topic, ser)
      de shouldBe request
    }

    "serialise and deserialise success responses (no undo)" in {
      val response =
        ActionResponse.of[Json](SagaId.random(),
                                ActionId.random(),
                                CommandId.random(),
                                false,
                                Result.success(Optional.empty()))
      val ser = serdes.response.serializer().serialize(topic, response)
      val de  = serdes.response.deserializer().deserialize(topic, ser)
      de shouldBe response
    }

    "serialise and deserialise success responses (with undo)" in {
      val response =
        ActionResponse.of[Json](
          SagaId.random(),
          ActionId.random(),
          CommandId.random(),
          false,
          Result.success(
            Optional.of(
              UndoCommand.of((UserCommand.Insert(UUID.randomUUID(), "", ""): UserCommand).asJson, "")))
        )
      val ser = serdes.response.serializer().serialize(topic, response)
      val de  = serdes.response.deserializer().deserialize(topic, ser)
      de shouldBe response
    }

    "serialise and deserialise failure responses" in {
      val response =
        ActionResponse.of[Json](SagaId.random(),
                                ActionId.random(),
                                CommandId.random(),
                                false,
                                Result.failure(SagaError.of(SagaError.Reason.InternalError, "error")))
      val ser = serdes.response.serializer().serialize(topic, response)
      val de  = serdes.response.deserializer().deserialize(topic, ser)
      de shouldBe response
    }
  }
}
