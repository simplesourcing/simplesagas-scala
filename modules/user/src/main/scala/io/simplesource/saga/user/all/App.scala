package io.simplesource.saga.user.all

object App {

  def main(args: Array[String]): Unit = {
    io.simplesource.saga.user.command.App.startCommandProcessor()
    io.simplesource.saga.user.action.App.startActionProcessors()
    io.simplesource.saga.user.saga.App.startSagaCoordinator()
  }
}
