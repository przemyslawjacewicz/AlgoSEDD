package pl.epsilondeltalimit.algosedd.read.badges

import pl.epsilondeltalimit.algosedd.read.FilePathProvider

object BadgesFilePathProvider extends FilePathProvider {
  override val fileName: String = "Badges.xml"
  override val id: String       = "pathToBadgesFile"
}
