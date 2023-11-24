package pl.epsilondeltalimit

import cats.Monad
import pl.epsilondeltalimit.dep.Dep
import pl.epsilondeltalimit.dep.Transformations._

package object algosedd {

  trait TransformationM extends Transformation {
    implicit val id: String
  }

  trait TransformationWithImplicitCatalogM extends TransformationWithImplicitCatalog {
    implicit val id: String
  }

  trait PutTransformationM extends PutTransformation {
    implicit val id: String
  }

  trait PutTransformationWithImplicitCatalogM extends PutTransformationWithImplicitCatalog {
    implicit val id: String
  }

  implicit def depMonad(implicit id: String): Monad[Dep] =
    new Monad[Dep] {
      override def flatMap[A, B](fa: Dep[A])(f: A => Dep[B]): Dep[B] =
        fa.flatMap(f)

      override def tailRecM[A, B](a: A)(f: A => Dep[Either[A, B]]): Dep[B] =
        flatMap(f(a)) {
          case Left(value)  => tailRecM(value)(f)
          case Right(value) => pure(value)
        }

      override def pure[A](x: A): Dep[A] =
        Dep.dep(id)(x)
    }

}
