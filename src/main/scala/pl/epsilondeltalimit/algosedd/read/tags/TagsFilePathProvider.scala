package pl.epsilondeltalimit.algosedd.read.tags

import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object TagsFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath").map("pathToTagsFile") { rootPath =>
        s"$rootPath/Tags.xml"
      }
    }

}
