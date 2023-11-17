package pl.epsilondeltalimit.algosedd.read.badges

import cats._
import pl.epsilondeltalimit.algosedd.{PutTransformationWithImplicitCatalogM, _}
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object BadgesFilePathProvider extends PutTransformationWithImplicitCatalogM {

  import Dep.implicits._

  override implicit val id: String = "pathToBadgesFile"

  val BadgesXmlFileName: String = "Badges.xml"

  override def apply(implicit c: Catalog): Dep[_] = {
    //    "rootPath".as[String].map(rootPath => s"$rootPath/$BadgesXmlFileName").as("pathToBadgesFile")
    Monad[Dep].map("rootPath".as[String])(rootPath => s"$rootPath/$BadgesXmlFileName")
  }

}
