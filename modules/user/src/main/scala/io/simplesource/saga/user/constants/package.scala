package io.simplesource.saga.user

package object constants {
  // sagas
  val sagaBaseName    = "saga"

  // action processors
  val sagaActionBaseName = "saga_action"

  // commands (simple sourcing)
  // user aggregate
  val userAggregateName = "user"
  val userActionType    = "sourcing_action_user"
  // account aggregate
  val accountAggregateName = "account"
  val accountActionType    = "sourcing_action_account"

  val asyncActionType = "async_test_action"
  val httpActionType = "http_action"

  val asyncTestTopic = "async_test_topic"
  val httpTopic = "fx_rates"

  val kafkaBootstrap = "localhost:9092"

  val commandTopicPrefix = "simple_sourcing-"

  val partitions    = 6
  val replication   = 1
  val retentionDays = 7
}
