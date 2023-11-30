package pl.epsilondeltalimit

import cats.Monad
import pl.epsilondeltalimit.dep._

import scala.annotation.tailrec
import scala.util.Random

package object algosedd {

  implicit val depMonad: Monad[Dep] = new Monad[Dep] {
    override def flatMap[A, B](fa: Dep[A])(f: A => Dep[B]): Dep[B] =
      fa.flatMap(f)

    @tailrec
    override def tailRecM[A, B](a: A)(f: A => Dep[Either[A, B]]): Dep[B] =
      f(a) match {
        case dep: LeafDep[_] => dep() match {
            case Left(value)  => tailRecM(value)(f)
            case Right(value) => LeafDep[B](dep.id, dep.needs, () => value)
          }
        case dep: BranchDep[_] => dep() match {
            case Left(value)  => tailRecM(value)(f)
            case Right(value) => BranchDep(dep.id, dep.needs, () => value)
          }
      }

    override def pure[A](x: A): Dep[A] =
      BranchDep(Random.alphanumeric.take(10).mkString, () => Set.empty, () => x)

  }

}
