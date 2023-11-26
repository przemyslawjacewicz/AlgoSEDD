package pl.epsilondeltalimit.algosedd.read.users

import pl.epsilondeltalimit.algosedd.read.FilePathProvider

object UsersFilePathProvider extends FilePathProvider {
  override val fileName: String = "Users.xml"
  override val id: String       = "pathToUsersFile"
}
