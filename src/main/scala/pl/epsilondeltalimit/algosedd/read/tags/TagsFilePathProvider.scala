package pl.epsilondeltalimit.algosedd.read.tags

import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object TagsFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath")
        .map { rootPath =>
          s"$rootPath/Tags.xml"
        }
        .as("pathToTagsFile")
    }

}
