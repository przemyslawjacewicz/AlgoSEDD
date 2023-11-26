package pl.epsilondeltalimit.algosedd.read.votes

import pl.epsilondeltalimit.algosedd.read.FilePathProvider

object VotesFilePathProvider extends FilePathProvider {
  override val fileName: String = "Votes.xml"
  override val id: String       = "pathToVotesFile"
}
