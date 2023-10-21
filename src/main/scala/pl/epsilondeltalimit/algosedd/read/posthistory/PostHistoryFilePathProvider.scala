package pl.epsilondeltalimit.algosedd.read.posthistory

import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object PostHistoryFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath")
        .map { rootPath =>
          s"$rootPath/PostHistory.xml"
        }
        .as("pathToPostHistoryFile")
    }

}
