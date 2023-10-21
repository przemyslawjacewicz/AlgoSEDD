package pl.epsilondeltalimit.algosedd.read.votes

import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object VotesFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath")
        .map { rootPath =>
          s"$rootPath/Votes.xml"
        }
        .as("pathToVotesFile")
    }

}
