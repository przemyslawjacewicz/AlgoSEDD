package pl.epsilondeltalimit.algosedd.read

import pl.epsilondeltalimit.dep.Transformations.PutTransformationWithImplicitCatalog
import pl.epsilondeltalimit.dep.{Catalog, Dep}

trait FilePathProvider extends PutTransformationWithImplicitCatalog {
  import Dep.implicits._

  val fileName: String
  val id: String

  override def apply(implicit c: Catalog): Dep[_] =
    "rootPath".as[String].map(rootPath => s"$rootPath/$fileName").as(id)

}
