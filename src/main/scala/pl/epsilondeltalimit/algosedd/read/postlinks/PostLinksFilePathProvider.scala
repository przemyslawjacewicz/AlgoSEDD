package pl.epsilondeltalimit.algosedd.read.postlinks

import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object PostLinksFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath")
        .map { rootPath =>
          s"$rootPath/PostLinks.xml"
        }
        .as("pathToPostLinksFile")
    }

}
