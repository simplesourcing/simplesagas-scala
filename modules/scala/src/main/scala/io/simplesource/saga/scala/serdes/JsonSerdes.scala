package io.simplesource.saga.scala.serdes

import java.util.{Optional, UUID}

import io.circe.{Decoder, Encoder}
import io.simplesource.api.CommandId
import io.simplesource.data.{Result, Sequence}
import io.simplesource.kafka.api.{AggregateSerdes, CommandSerdes}
import io.simplesource.kafka.model._
import io.simplesource.saga.model.action.{ActionCommand, ActionId, ActionStatus, SagaAction, UndoCommand}
import io.simplesource.saga.model.saga
import io.simplesource.saga.model.saga.{SagaError, SagaId}
import io.simplesource.saga.model.serdes.{ActionSerdes, SagaSerdes}
import org.apache.kafka.common.serialization.Serde

object JsonSerdes {
  import ProductCodecs._
  import JavaCodecs._

  private def commandRequest[K: Encoder: Decoder, C: Encoder: Decoder] =
    productCodecs4[UUID, K, Long, C, CommandRequest[K, C]]("key", "command", "readSequence", "commandId")(
      v => (v.commandId().id, v.aggregateKey(), v.readSequence().getSeq, v.command()),
      (id, k, rs, c) => new CommandRequest(CommandId.of(id), k, Sequence.position(rs), c)
    ).asSerde

  def commandSerdes[K: Encoder: Decoder, C: Encoder: Decoder]: CommandSerdes[K, C] =
    new CommandSerdes[K, C] {

      private val aks = serdeFromCodecs[K]
      private val crs = JsonSerdes.commandRequest[K, C]

      private val cid = mappedCodec[UUID, CommandId](_.id, CommandId.of).asSerde
      private val cr  = ResultEncoders.cr[K]

      override def aggregateKey(): Serde[K]                      = aks
      override def commandRequest(): Serde[CommandRequest[K, C]] = crs
      override def commandId(): Serde[CommandId]                 = cid
      override def commandResponse(): Serde[CommandResponse[K]]  = cr
    }

  class SharedCodecs[A: Encoder: Decoder] {

    import ProductCodecs._
    implicit val (sidEnd, sidDec) = mappedCodec[UUID, SagaId](_.id, SagaId.of)
    val sid: Serde[SagaId]        = (sidEnd, sidDec).asSerde

    implicit val (aidEnd, aidDec) =
      mappedCodec[UUID, ActionId](_.id, ActionId.of)
    val aid: Serde[ActionId] = (aidEnd, aidDec).asSerde

    implicit val (cidEnd, cidDec) =
      mappedCodec[UUID, CommandId](_.id, CommandId.of)
    val cid: Serde[CommandId] = (cidEnd, cidDec).asSerde

    implicit val (ucd, uce) =
      productCodecs2[A, String, UndoCommand[A]]("command", "actionType")(x => (x.command, x.actionType),
                                                                         (c, at) => UndoCommand.of[A](c, at))

  }

  def aggregateSerdes[K: Encoder: Decoder, C: Encoder: Decoder, E: Encoder: Decoder, A: Encoder: Decoder]
    : AggregateSerdes[K, C, E, A] =
    new SharedCodecs[A] with AggregateSerdes[K, C, E, A] {

      private val cs = commandSerdes[K, C]

      private val vwss =
        productCodecs2[E, Long, ValueWithSequence[E]]("value", "sequence")(
          v => (v.value(), v.sequence().getSeq),
          (v, s) => new ValueWithSequence(v, Sequence.position(s))
        ).asSerde

      private val au  = ResultEncoders.au[A]
      private val aus = au.asSerde

      override def aggregateKey(): Serde[K] = cs.aggregateKey()
      override def commandRequest(): Serde[CommandRequest[K, C]] =
        cs.commandRequest()
      override def commandId(): Serde[CommandId]                    = cs.commandId()
      override def valueWithSequence(): Serde[ValueWithSequence[E]] = vwss
      override def aggregateUpdate(): Serde[AggregateUpdate[A]]     = aus
      override def commandResponse(): Serde[CommandResponse[K]] =
        cs.commandResponse()
    }

  def actionSerdes[A: Encoder: Decoder]: ActionSerdes[A] =
    new SharedCodecs[A] with ActionSerdes[A] {
      import io.simplesource.saga.model.messages._

      private val req =
        productCodecs6[UUID, UUID, UUID, A, String, Boolean, ActionRequest[A]]("sagaId",
                                                                               "actionId",
                                                                               "commandId",
                                                                               "command",
                                                                               "isUndo",
                                                                               "actionType")(
          v =>
            (v.sagaId.id,
             v.actionId.id,
             v.actionCommand.commandId.id,
             v.actionCommand.command,
             v.actionCommand.actionType,
             v.isUndo),
          (sId, aId, cId, c, at, u) =>
            ActionRequest.of(SagaId.of(sId),
                             ActionId.of(aId),
                             ActionCommand.of[A](CommandId.of(cId), c, at),
                             u)
        ).asSerde

      import ResultEncoders._
      private def resp: Serde[ActionResponse[A]] = {
        productCodecs5[UUID,
                       UUID,
                       UUID,
                       Boolean,
                       Result[SagaError, Optional[UndoCommand[A]]],
                       ActionResponse[A]]("sagaId", "actionId", "commandId", "isUndo", "result")(
          x => (x.sagaId.id, x.actionId.id, x.commandId.id, x.isUndo, x.result),
          (sagaId, actionId, commandId, isUndo, result) =>
            ActionResponse
              .of[A](SagaId.of(sagaId), ActionId.of(actionId), CommandId.of(commandId), isUndo, result)
        ).asSerde
      }

      override def commandId(): Serde[CommandId]        = cid
      override def sagaId(): Serde[SagaId]              = sid
      override def actionId(): Serde[ActionId]          = aid
      override def request(): Serde[ActionRequest[A]]   = req
      override def response(): Serde[ActionResponse[A]] = resp
    }

  def sagaSerdes[A: Encoder: Decoder]: SagaSerdes[A] =
    new SharedCodecs[A] with SagaSerdes[A] {
      import io.simplesource.saga.model.messages._
      import io.simplesource.saga.model.saga._
      import ResultEncoders._

      private implicit val (acEnc, acDec) =
        productCodecs3[UUID, A, String, ActionCommand[A]]("commandId", "command", "actionType")(
          x => (x.commandId.id, x.command, x.actionType),
          (cid, c, at) => ActionCommand.of[A](CommandId.of(cid), c, at))

      Set(1).map(identity)

      private implicit val (saEnc, saDec) =
        productCodecs7[UUID,
                       ActionCommand[A],
                       Optional[ActionCommand[A]],
                       java.util.Set[ActionId],
                       String,
                       java.util.List[SagaError],
                       Int,
                       SagaAction[A]]("actionId",
                                      "command",
                                      "undoCommand",
                                      "dependencies",
                                      "status",
                                      "error",
                                      "retryCount")(
          x =>
            (x.actionId.id,
             x.command,
             x.undoCommand,
             x.dependencies,
             x.status.toString,
             x.error,
             x.retryCount),
          (aid, c, uc, d, s, e, rc) =>
            SagaAction
              .of[A](ActionId.of(aid), c, uc, d, ActionStatus.valueOf(s), e, rc)
        )

      private implicit val (sagaEnc, sagaDec) =
        productCodecs4[SagaId, java.util.Map[ActionId, SagaAction[A]], String, Sequence, Saga[A]](
          "sagaId",
          "actions",
          "status",
          "sequence"
        )(x => (x.sagaId, x.actions, x.status.toString, x.sequence),
          (sid, acts, st, seq) => Saga.of[A](sid, acts, SagaStatus.valueOf(st), seq))

      private val sagaSerde = (sagaEnc, sagaDec).asSerde

      private val sagaRequestSerde =
        productCodecs2[SagaId, Saga[A], SagaRequest[A]]("sagaId", "initialState")(
          x => (x.sagaId, x.initialState),
          (id, init) => SagaRequest.of[A](id, init)
        ).asSerde

      import ResultEncoders._
      private val sagaResponseSerde =
        productCodecs2[SagaId, Result[SagaError, Sequence], SagaResponse]("sagaId", "initialState")(
          x => (x.sagaId, x.result),
          (id, init) => SagaResponse.of(id, init)
        ).asSerde

      private implicit val (initialEnc, initialDec) =
        mappedCodec[Saga[A], SagaStateTransition.SetInitialState[A]](
          _.sagaState,
          SagaStateTransition.SetInitialState.of[A])

      private implicit val (ascEnc, ascDec) =
        productCodecs6[SagaId,
                       ActionId,
                       String,
                       java.util.List[SagaError],
                       Optional[UndoCommand[A]],
                       Boolean,
                       SagaStateTransition.SagaActionStateChanged[A]](
          "sagaId",
          "actionId",
          "actionStatus",
          "error",
          "undoCommand",
          "isUndo"
        )(
          x => (x.sagaId, x.actionId, x.actionStatus.toString, x.actionErrors, x.undoCommand, x.isUndo),
          (sid, aid, st, e, uc, isUndo) =>
            SagaStateTransition.SagaActionStateChanged
              .of(sid, aid, ActionStatus.valueOf(st), e, uc, isUndo)
        )

      private implicit val (sscEnc, sscDec) =
        productCodecs3[SagaId, String, java.util.List[SagaError], SagaStateTransition.SagaStatusChanged[A]](
          "sagaId",
          "sagaStatus",
          "errors"
        )(x => (x.sagaId, x.sagaStatus.toString, x.sagaErrors),
          (sid, ss, es) => SagaStateTransition.SagaStatusChanged.of(sid, SagaStatus.valueOf(ss), es))

      private implicit val (tlEnc, tlDec) =
        mappedCodec[java.util.List[SagaStateTransition.SagaActionStateChanged[A]],
                    SagaStateTransition.TransitionList[A]](_.actions,
                                                           x => SagaStateTransition.TransitionList.of(x))

      private implicit val stateTransitionSerde =
        productCodecs4[
          Option[SagaStateTransition.SetInitialState[A]],
          Option[SagaStateTransition.SagaActionStateChanged[A]],
          Option[SagaStateTransition.SagaStatusChanged[A]],
          Option[SagaStateTransition.TransitionList[A]],
          SagaStateTransition[A]
        ]("initial", "actionStatus", "sagaStatus", "transitionList")(
          {
            case x: SagaStateTransition.SetInitialState[A] =>
              (Some(x), None, None, None)
            case x: SagaStateTransition.SagaActionStateChanged[A] =>
              (None, Some(x), None, None)
            case x: SagaStateTransition.SagaStatusChanged[A] =>
              (None, None, Some(x), None)
            case x: SagaStateTransition.TransitionList[A] =>
              (None, None, None, Some(x))
          },
          (is, as, ss, tl) =>
            (is, as, ss, tl) match {
              case (Some(x), _, _, _) => x
              case (_, Some(x), _, _) => x
              case (_, _, Some(x), _) => x
              case (_, _, _, Some(x)) => x
              case _ =>
                throw new Exception("Error in SagaStateTransition deserialization")
          }
        ).asSerde

      override def sagaId(): Serde[SagaId]          = sid
      override def request(): Serde[SagaRequest[A]] = sagaRequestSerde
      override def response(): Serde[SagaResponse]  = sagaResponseSerde
      override def state(): Serde[saga.Saga[A]]     = sagaSerde
      override def transition(): Serde[SagaStateTransition[A]] =
        stateTransitionSerde
    }
}
