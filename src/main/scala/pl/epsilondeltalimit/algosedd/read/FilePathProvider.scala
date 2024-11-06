package pl.epsilondeltalimit.algosedd.read

import pl.epsilondeltalimit.algosedd.read.implicits._
import pl.epsilondeltalimit.dep.catalog.Catalog
import pl.epsilondeltalimit.dep.dep.Result
import pl.epsilondeltalimit.dep.transformation.ResultTransformationWithImplicitCatalog

trait FilePathProvider extends ResultTransformationWithImplicitCatalog[String] {
  val fileName: String
  val id: String

  override def apply(implicit c: Catalog): Result[String] =
    "rootPath".as[String].map(_ / fileName).as(id)

}
