package pl.epsilondeltalimit.algosedd.read

import pl.epsilondeltalimit.algosedd.implicits._
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.dep.Result
import pl.epsilondeltalimit.dep.transformation.implicits._

trait FilePathProvider extends (Catalog => Result[String]) {
  val fileName: String
  val id: String

  override def apply(c: Catalog): Result[String] = {
    implicit val _c: Catalog = c

    "rootPath".as[String].map(_ / fileName).as(id)
  }
}
