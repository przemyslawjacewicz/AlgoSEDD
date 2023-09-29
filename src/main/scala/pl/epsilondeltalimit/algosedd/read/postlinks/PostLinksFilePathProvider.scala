package pl.epsilondeltalimit.algosedd.read.postlinks

import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object PostLinksFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath").map("pathToPostLinksFile") { rootPath =>
        s"$rootPath/PostLinks.xml"
      }
    }

}
