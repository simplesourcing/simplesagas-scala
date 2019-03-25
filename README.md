# Simple Sagas - Scala
[![Build Status](https://travis-ci.com/simplesourcing/simplesagas-scala.svg?branch=master)](https://travis-ci.com/simplesourcing/simplesagas-scala)

This repo has been formed by splitting the Scala code from the main [Simple Sagas](https://github.com/simplesourcing/simplesagas) repo.

### Saga DSL

A simple DSL is provided to simplify creating sagas, loosely based on the [Akka Streams Graph DSL](https://doc.akka.io/docs/akka/2.5/stream/stream-graphs.html)

1. Create a builder:
    ```scala
    import io.simplesource.saga.scala.dsl._
   
    val builder = SagaBuilder[A]()
    ```

2. Add some actions:

    ```scala
    val a = builder.addAction(...)
    val b = builder.addAction(...)
    val c = builder.addAction(...)
    val d = builder.addAction(...)
    val e = builder.addAction(...)
    ```

3. Defined dependencies between actions:

    **Examples:**
    
    Execute `a`, then `b`, then `c`:
    ```scala
    a ~> b ~> c
    ```
    
    ----------
    
    Execute `a`, then `b`, `c` and `d` in parallel, then `e`:
    ```scala
    a ~> b ~> e
    a ~> c ~> e
    a ~> d ~> e
    ```
    This can also be expressed as:
    ```scala
    a ~> inParallel(b, c, d) ~> e
    a ~> List(b, c, d).inParallel ~> e
    ```
    
    ----------
    
    Execute `a`, then `b`, `c` and `d` in series, then `e`. The following are equivalent:

    ```scala
    a ~> b ~> c ~> d ~> e
    inSeries(a, b, c) ~> d ~> e
    inSeries(a, b, c, d, e)
    a ~> inSeries(b, c, d) ~> e
    a ~> List(b, c, d).inSeries ~> e
    ```
    This is useful when we have a sequence of actions of length only known at runtime.
    
    Note that the `~>` operator is associative, so 
    `(a ~> b) ~> c` is equivalent to `a ~> (b ~> c)`.
   
4. Build the immutable sagas graph:
   
   ```scala
   val eitherSaga: Either[SagaError, Saga[A]] = builder.build()
   ```

