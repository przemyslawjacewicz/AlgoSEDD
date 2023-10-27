package pl.epsilondeltalimit.algosedd.read.badges

import pl.epsilondeltalimit.dep.Transformations.PutTransformationWithImplicitCatalog
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object BadgesFilePathProvider extends PutTransformationWithImplicitCatalog {

  import Dep.implicits._

  val BadgesXmlFileName: String = "Badges.xml"

  override def apply(implicit c: Catalog): Dep[_] =
    "rootPath".as[String].map(rootPath => s"$rootPath/$BadgesXmlFileName").as("pathToBadgesFile")

}
