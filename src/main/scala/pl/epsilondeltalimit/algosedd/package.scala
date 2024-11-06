package pl.epsilondeltalimit

import cats.Monad
import pl.epsilondeltalimit.dep.dep.{Dep, Part, Result}

import scala.annotation.tailrec
import scala.util.Random

package object algosedd {

  object implicits {
    // todo: add monad law tests for this monad
    implicit val depMonad: Monad[Dep] = new Monad[Dep] {
      override def flatMap[A, B](fa: Dep[A])(f: A => Dep[B]): Dep[B] =
        fa.flatMap(f)

      @tailrec
      override def tailRecM[A, B](a: A)(f: A => Dep[Either[A, B]]): Dep[B] =
        f(a) match {
          case dep: Result[_] => dep() match {
              case Left(value)  => tailRecM(value)(f)
              case Right(value) => dep.copy(value = () => value)
            }
          case dep: Part[_] => dep() match {
              case Left(value)  => tailRecM(value)(f)
              case Right(value) => dep.copy(value = () => value)
            }
        }

      override def pure[A](x: A): Dep[A] =
        Dep(Random.alphanumeric.take(10).mkString)(x)

    }

  }

}
