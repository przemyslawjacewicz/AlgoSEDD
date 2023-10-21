package pl.epsilondeltalimit.algosedd.read.users

import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

object UsersFilePathProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[String]("rootPath")
        .map { rootPath =>
          s"$rootPath/Users.xml"
        }
        .as("pathToUsersFile")
    }

}
