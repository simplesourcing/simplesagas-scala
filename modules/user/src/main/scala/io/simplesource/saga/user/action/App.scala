package io.simplesource.saga.user.action

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.{Optional, UUID}

import io.circe.Json
import io.circe.generic.auto._
import io.simplesource.data.{Result, Sequence}
import io.simplesource.saga.action.ActionApp
import io.simplesource.saga.action.async.AsyncResult
import io.simplesource.saga.action.async.{AsyncBuilder, AsyncSpec, Callback}
import io.simplesource.saga.action.http.{HttpBuilder, HttpOutput, HttpRequest, HttpSpec}
import io.simplesource.saga.action.eventsourcing.{EventSourcingBuilder, EventSourcingSpec}
import io.simplesource.saga.model.serdes.TopicSerdes
import io.simplesource.saga.scala.serdes.{JsonSerdes, ProductCodecs}
import io.simplesource.saga.shared.streams.StreamAppConfig
import io.simplesource.saga.shared.topics.TopicConfigBuilder
import io.simplesource.saga.user.command.model.auction.{AccountCommand, AccountCommandInfo}
import io.simplesource.saga.user.command.model.user.{UserCommand, UserCommandInfo}
import io.simplesource.saga.user.constants
import org.apache.kafka.common.serialization.Serdes

import scala.collection.JavaConverters._

object App {
  val appConfig =
    StreamAppConfig.of("action-processor-1", "127.0.0.1:9092")

  def main(args: Array[String]): Unit = {
    startActionProcessors()
  }

  def startActionProcessors(): Unit = {
    val actionTopicBuilder: TopicConfigBuilder.BuildSteps = a => a
    val commandTopicBuilder: TopicConfigBuilder.BuildSteps = a =>
      a.withTopicPrefix(constants.commandTopicPrefix)
    ActionApp
      .of[Json](JsonSerdes.actionSerdes[Json])
      .withActionProcessor(EventSourcingBuilder
        .apply(accountSpec, actionTopicBuilder, commandTopicBuilder))
      .withActionProcessor(EventSourcingBuilder
        .apply(userSpec, actionTopicBuilder, commandTopicBuilder))
      .withActionProcessor(AsyncBuilder.apply(asyncSpec))
      .withActionProcessor(HttpBuilder.apply(httpSpec))
      .run(appConfig)
  }

  implicit class EOps[E, A](eea: Either[E, A]) {
    def toResult: Result[E, A] =
      eea.fold(e => Result.failure(e), a => Result.success(a))
  }

  lazy val userSpec =
    EventSourcingSpec.of[Json, UserCommandInfo, UUID, UserCommand](
      constants.userActionType,
      "user",
      json => json.as[UserCommandInfo].toResult.errorMap(e => e),
      _.command,
      _.userId,
      i => Sequence.position(i.sequence),
      (_, _) => Optional.empty(),
      JsonSerdes.commandSerdes[UUID, UserCommand],
      Duration.of(20, ChronoUnit.SECONDS),
    )

  lazy val accountSpec =
    EventSourcingSpec.of[Json, AccountCommandInfo, UUID, AccountCommand](
      constants.accountActionType,
      "account",
      json => json.as[AccountCommandInfo].toResult.errorMap(e => e),
      _.command,
      _.accountId,
      i => Sequence.position(i.sequence),
      (_, _) => Optional.empty(),
      JsonSerdes.commandSerdes[UUID, AccountCommand],
      Duration.ofSeconds(30)
    )

  lazy val asyncSpec = AsyncSpec.of[Json, String, String, String, String](
    constants.asyncActionType,
    (a: Json) => {
      val decoded = a.as[String]
      decoded.toResult.errorMap(e => e)
    },
    (i: String, callBack: Callback[String]) => {
      callBack.complete(Result.success(s"${i.length.toString}: $i"))
    }, //i => Future.successful(s"${i.length.toString}: $i"),
    appConfig.appId,
    Optional.of(
      AsyncResult.of(
        o => Optional.of(Result.success(o)),
        i => i.toLowerCase.take(3),
        (_, _, _) => Optional.empty(),
        Optional.of(new TopicSerdes(Serdes.String(), Serdes.String())),
      )),
    Optional.of(Duration.ofSeconds(60))
  )

  // Http currency fetch example
  final case class Key(id: String)
  type Body   = Option[String]
  type Input  = Json
  type Output = Json
  final case class FXRates(date: String, base: String, rates: Map[String, BigDecimal])

  import io.circe.generic.auto._
  import scala.concurrent.ExecutionContext.Implicits.global
  implicit val decoder = HttpClient.httpRequest[Key, Body]._2

  lazy val httpSpec = HttpSpec.of[Input, Key, Body, Output, FXRates](
    constants.httpActionType,
    _.as[HttpRequest[Key, Body]].toResult.map(x => x).errorMap(e => e),
    HttpClient.requester[Key, Body, Output],
    appConfig.appId,
    Optional.of(
      HttpOutput.of(
        (o: Input) => Optional.of(o.as[FXRates].toResult.errorMap(e => e)),
        Optional.of(
          new TopicSerdes(ProductCodecs.serdeFromCodecs[Key], ProductCodecs.serdeFromCodecs[FXRates])),
        (_, _) => Optional.empty()
      )),
    Optional.of(Duration.of(60, ChronoUnit.SECONDS))
  )
}
