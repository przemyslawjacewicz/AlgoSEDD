package pl.epsilondeltalimit.algosedd

import org.apache.spark.sql.functions.lit
import pl.epsilondeltalimit.dep.Catalog
import pl.epsilondeltalimit.dep.Transformations.Transformation

import java.time.LocalDate

//TODO: customize logging - add start and finish msg
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

    // todo: convert to reflection-based transformation retrieval
    val transformations: Set[Transformation] = Set(
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
      write.RelativePopularityByAggregationIntervalAndTagStorage,
      write.TagsStorage
    )

//    val output = transformations.foldLeft(catalog)((c, t) => t(c))
    val output = catalog
      .withTransformations(transformations.toSeq: _*)

    output.eval[Unit]("relativePopularityByAggregationIntervalAndTagStorage")

    logger.warn("Done. Exiting.")
  }
}
