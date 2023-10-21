package pl.epsilondeltalimit.algosedd.read.badges

import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object BadgesFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath")
        .map(rootPath => s"$rootPath/Badges.xml")
        .as("pathToBadgesFile")
    }
}
