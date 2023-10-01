package pl.epsilondeltalimit.algosedd

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit
import pl.epsilondeltalimit.dep.v6_1.Catalog

import java.time.LocalDate

//TODO: customize logging - add start and finish msg
//TODO: optimize spark execution
object AlgoSEDD extends Logging {

  def main(args: Array[String]): Unit = {

    val catalog = new Catalog

    val startDate = args(0)
    catalog.unit("startDate")(LocalDate.parse(startDate))

    val endDate = args(1)
    catalog.unit("endDate")(LocalDate.parse(endDate))

    val aggregationInterval = args(2)
    catalog.unit("aggregationInterval")(aggregationInterval)

    val rootPath = args(3)
    catalog.unit("rootPath")(rootPath)

    val pathToOutput = args(4)
    catalog.unit("pathToOutput")(pathToOutput)

    // todo: remove me
    // NOTE: date filter applicable only for testing
    //    val dateFilter = $"creation_date" > "2017-12-31" && $"creation_date" < "2019-01-01"
    //    val dateFilter = $"creation_date" > "2019-05-31" && $"creation_date" < "2019-07-01"
    val dateFilter = lit(true)

    //    logger.warn("Reading data from dump files.")
    //    val pathToPostsFile = s"$rootPath/Posts.xml"
    //    val pathToTagsFile = s"$rootPath/Tags.xml"
    //    val pathToUsersFile = s"$rootPath/Users.xml"
    //    val pathToVotesFile = s"$rootPath/Votes.xml"

    // todo: convert to reflection-based transformation retrieval
    val transformations = Set(
      SparkSessionProvider,
      read.badges.BadgesFilePathProvider,
      read.badges.BadgesFileContentProvider,
      read.comments.CommentsFilePathProvider,
      read.comments.CommentsFileContentProvider,
      read.posthistory.PostHistoryFilePathProvider,
      read.posthistory.PostHistoryFileContentProvider,
      read.postlinks.PostLinksFilePathProvider,
      read.postlinks.PostLinksFileContentProvider,
      read.posts.PostsFilePathProvider,
      read.posts.PostsFileContentProvider,
      read.tags.TagsFilePathProvider,
      read.tags.TagsFileContentProvider,
      read.users.UsersFilePathProvider,
      read.users.UsersFileContentProvider,
      read.votes.VotesFilePathProvider,
      read.votes.VotesFileContentProvider,
      analyze.TagsByQuestionPostId,
      analyze.TagsByAnswerPostId,
      analyze.TagsByPostId,
      analyze.TagsByCreationDataFromQuestions,
      analyze.TagsByCreationDataFromAnswers,
      analyze.TagsByCreationDataFromComments,
      analyze.TagsByCreationDataFromVotes,
      analyze.TagsByCreationDataFromPostHistory,
      analyze.TagsByCreationDataFromPostLinks,
      analyze.DataEntriesCountByCreationDateAndTag,
      analyze.Days,
      analyze.DaysAndTags,
      analyze.EntriesCountByCreationDateAndTag,
      analyze.EntriesCountByAggregationIntervalAndTag,
      analyze.EntriesCountByAggregationInterval,
      analyze.EntriesCount,
      analyze.RelativePopularityByAggregationIntervalAndTag,
      analyze.RelativePopularityByAggregationIntervalAndTagStorage,
      analyze.Tags
    )

    val output = transformations.foldLeft(catalog)((_c, _t) => _t(_c))

    output.eval[Unit]("RelativePopularityByAggregationIntervalAndTagStorage")

    logger.warn("Done. Exiting.")
  }
}
