package io.simplesource.saga.user.action

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.{Optional, UUID}

import io.circe.Json
import io.circe.generic.auto._
import io.simplesource.data.{Result, Sequence}
import io.simplesource.kafka.spec.TopicSpec
import io.simplesource.saga.action.ActionApp
import io.simplesource.saga.action.async.{AsyncBuilder, AsyncOutput, AsyncSpec, Callback}
import io.simplesource.saga.action.http.{HttpBuilder, HttpOutput, HttpRequest, HttpSpec}
import io.simplesource.saga.action.eventsourcing.{EventSourcingBuilder, EventSourcingSpec}
import io.simplesource.saga.model.serdes.TopicSerdes
import io.simplesource.saga.scala.serdes.{JsonSerdes, ProductCodecs}
import io.simplesource.saga.shared.streams.StreamAppConfig
import io.simplesource.saga.shared.topics.{TopicConfigBuilder, TopicCreation}
import io.simplesource.saga.user.command.model.auction.{AccountCommand, AccountCommandInfo}
import io.simplesource.saga.user.command.model.user.{UserCommand, UserCommandInfo}
import io.simplesource.saga.user.constants
import org.apache.kafka.common.serialization.Serdes

import scala.collection.JavaConverters._

object App {
  val appConfig =
    new StreamAppConfig("action-processor-1", "127.0.0.1:9092")

  def main(args: Array[String]): Unit = {
    startActionProcessors()
  }

  def startActionProcessors(): Unit = {
    val  actionTopicBuilder: TopicConfigBuilder.BuildSteps = a => a
    val  commandTopicBuilder: TopicConfigBuilder.BuildSteps = a => a.withTopicPrefix(constants.commandTopicPrefix)
    ActionApp.of[Json](JsonSerdes.actionSerdes[Json], Duration.ofDays(1))
      .withActionProcessor(EventSourcingBuilder.apply(accountSpec, actionTopicBuilder, commandTopicBuilder))
      .withActionProcessor(EventSourcingBuilder.apply(userSpec, actionTopicBuilder, commandTopicBuilder))
      .withActionProcessor(AsyncBuilder.apply(asyncSpec))
      .withActionProcessor(HttpBuilder.apply(httpSpec))
      .run(appConfig)
  }

  implicit class EOps[E, A](eea: Either[E, A]) {
    def toResult: Result[E, A] =
      eea.fold(e => Result.failure(e), a => Result.success(a))
  }

  lazy val userSpec = new EventSourcingSpec[Json, UserCommandInfo, UUID, UserCommand](
    constants.userActionType,
    json => json.as[UserCommandInfo].toResult.errorMap(e => e),
    _.command,
    _.userId,
    i => Sequence.position(i.sequence),
    JsonSerdes.commandSerdes[UUID, UserCommand],
    Duration.of(20, ChronoUnit.SECONDS),
    "user"
  )

  lazy val accountSpec =
    new EventSourcingSpec[Json, AccountCommandInfo, UUID, AccountCommand](
      constants.accountActionType,
      json => json.as[AccountCommandInfo].toResult.errorMap(e => e),
      _.command,
      _.accountId,
      i => Sequence.position(i.sequence),
      JsonSerdes.commandSerdes[UUID, AccountCommand],
      Duration.ofSeconds(30),
      "account"
    )

  lazy val asyncSpec = new AsyncSpec[Json, String, String, String, String](
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
      new AsyncOutput(
        o => Optional.of(Result.success(o)),
        new TopicSerdes(Serdes.String(), Serdes.String()),
        i => i.toLowerCase.take(3),
        _ => Optional.of(constants.asyncTestTopic),
        List(new TopicCreation(constants.asyncTestTopic, new TopicSpec(6, 1, Map.empty[String, String].asJava))).asJava
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

  lazy val httpSpec = new HttpSpec[Input, Key, Body, Output, FXRates](
    constants.httpActionType,
    _.as[HttpRequest[Key, Body]].toResult.map(x => x).errorMap(e => e),
    HttpClient.requester[Key, Body, Output],
    appConfig.appId,
    Optional.of(
      new HttpOutput(
        (o: Input) => Optional.of(o.as[FXRates].toResult.errorMap(e => e)),
        new TopicSerdes(ProductCodecs.serdeFromCodecs[Key], ProductCodecs.serdeFromCodecs[FXRates]),
        List(new TopicCreation(constants.httpTopic, new TopicSpec(6, 1, Map.empty[String, String].asJava))).asJava
      )),
    Optional.of(Duration.of(60, ChronoUnit.SECONDS))
  )
}
