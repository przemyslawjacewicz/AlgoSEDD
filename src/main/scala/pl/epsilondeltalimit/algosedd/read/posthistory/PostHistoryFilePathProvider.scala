package pl.epsilondeltalimit.algosedd.read.posthistory

import pl.epsilondeltalimit.algosedd.read.FilePathProvider

object PostHistoryFilePathProvider extends FilePathProvider {
  override val fileName: String = "PostHistory.xml"
  override val id: String       = "pathToPostHistoryFile"
}
