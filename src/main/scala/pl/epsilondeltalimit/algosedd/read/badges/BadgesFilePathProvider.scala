package pl.epsilondeltalimit.algosedd.read.badges

import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object BadgesFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath").map("pathToBadgesFile") { rootPath =>
        s"$rootPath/Badges.xml"
      }
    }
}
