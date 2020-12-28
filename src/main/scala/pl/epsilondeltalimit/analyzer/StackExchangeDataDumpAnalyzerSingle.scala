package pl.epsilondeltalimit.analyzer

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import pl.epsilondeltalimit.analyzer.analyze.AnalyzeSupport._
import pl.epsilondeltalimit.analyzer.read._

//TODO: customize logging - add start and finish msg
object StackExchangeDataDumpAnalyzerSingle {
  private[this] val logger = Logger.getLogger(StackExchangeDataDumpAnalyzerSingle.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val aggregationInterval = args(0)
    val pathToBadgesFile = args(1)
    val pathToCommentsFile = args(2)
    val pathToPostHistoryFile = args(3)
    val pathToPostLinksFile = args(4)
    val pathToPostsFile = args(5)
    val pathToTagsFile = args(6)
    val pathToUsersFile = args(7)
    val pathToVotesFile = args(8)
    val pathToOutput = args(9)

    val conf = new SparkConf()
    conf.setAppName(StackExchangeDataDumpAnalyzerSingle.getClass.getSimpleName)
//    conf.setMaster("local[2]") //TODO: set to proper value when run on cluster

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()
    import spark.implicits._

    // NOTE: date filter applicable only for testing
    //    val dateFilter = $"creation_date" > "2017-12-31" && $"creation_date" < "2019-01-01"
    //    val dateFilter = $"creation_date" > "2019-05-31" && $"creation_date" < "2019-07-01"
    val dateFilter = lit(true)

    logger.warn("Reading data from dump files.")
    val badges = BadgesFileReadSupport.read(spark, pathToBadgesFile)
    val comments = CommentsFileReadSupport.read(spark, pathToCommentsFile)
    val postHistory = PostHistoryFileReadSupport.read(spark, pathToPostHistoryFile)
    val postLinks = PostLinksFileReadSupport.read(spark, pathToPostLinksFile)
    val posts = PostsFileReadSupport.read(spark, pathToPostsFile)
    val tags = TagsFileReadSupport.read(spark, pathToTagsFile)
    val users = UsersFileReadSupport.read(spark, pathToUsersFile)
    val votes = VotesFileReadSupport.read(spark, pathToVotesFile)

    logger.warn("Creating tags by post id map.")
    val tagsByPostId = posts.as("postsL")
      .join(posts.as("postsR"), when($"postsL.tags".isNotNull, $"postsL.id" === $"postsR.id").otherwise($"postsL.parent_id" === $"postsR.id"))
      .select(
        $"postsL.id".as("post_id"),
        $"postsR.tags".as("tags")
      )
    //    tagsByPostId.orderBy($"post_id".asc).show() //TODO: remove when implementation is finished

    logger.warn("Creating tags by creation date from questions.")
    val tagsByCreationDataFromQuestions = posts.as("posts")
      .where(postRecordIsQuestion)
      .join(tagsByPostId.as("tagsByPostId"), $"posts.id" === $"tagsByPostId.post_id")
      .select($"creation_date", $"tagsByPostId.tags")
    //    tagsByCreationDataFromQuestions.show() //TODO: remove when implementation is finished

    logger.warn("Creating tags by creation date from answers.")
    val tagsByCreationDataFromAnswers = posts.as("posts")
      .where(postRecordIsAnswer)
      .join(tagsByPostId.as("tagsByPostId"), $"posts.id" === $"tagsByPostId.post_id")
      .select($"creation_date", $"tagsByPostId.tags")
    //    tagsByCreationDataFromAnswers.show() //TODO: remove when implementation is finished

    logger.warn("Creating tags by post creation date from comments.")
    val tagsByCreationDataFromComments = comments
      .join(tagsByPostId, "post_id")
      .select($"creation_date", $"tags")
    //    tagsByCreationDataFromComments.show() //TODO: remove when implementation is finished

    logger.warn("Creating tags by post creation date from votes.")
    val tagsByCreationDataFromVotes = votes
      .join(tagsByPostId, "post_id")
      .select($"creation_date", $"tags")
    //    tagsByCreationDataFromVotes.show() //TODO: remove when implementation is finished

    logger.warn("Creating tags by post creation date from post history.")
    val tagsByCreationDataFromPostHistory = postHistory
      .join(tagsByPostId, "post_id")
      .select($"creation_date", $"tags")
    //    tagsByCreationDataFromPostHistory.show() //TODO: remove when implementation is finished

    logger.warn("Creating tags by post creation date from post links.")
    val tagsByCreationDataFromPostLinks = postLinks
      .join(tagsByPostId, "post_id")
      .select($"creation_date", $"tags")
    //    tagsByCreationDataFromPostLinks.show() //TODO: remove when implementation is finished

    logger.warn("Counting entries by creation date and tag.")
    val entriesCountByCreationDateAndTag = countEntriesByCreationDateAndTag(tagsByCreationDataFromQuestions)
      .withColumnRenamed("count", "question_entries_count_for_day_and_tag")
      .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromAnswers), Seq("creation_date", "tag"))
      .withColumnRenamed("count", "answer_entries_count_for_day_and_tag")
      .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromComments), Seq("creation_date", "tag"))
      .withColumnRenamed("count", "comment_entries_count_for_day_and_tag")
      .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromVotes), Seq("creation_date", "tag"))
      .withColumnRenamed("count", "vote_entries_count_for_day_and_tag")
      .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromPostHistory), Seq("creation_date", "tag"))
      .withColumnRenamed("count", "post_history_entries_count_for_day_and_tag")
      .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromPostLinks), Seq("creation_date", "tag"))
      .withColumnRenamed("count", "post_link_entries_count_for_day_and_tag")
    //    entriesCountByCreationDateAndTag.show() //TODO: remove when implementation is finished

    logger.warn("Aggregating entries by aggregation interval and tag.")
    val entriesCountByAggregationIntervalAndTag = entriesCountByCreationDateAndTag
      .groupBy(window($"creation_date", aggregationInterval).as("aggregation_interval"), $"tag")
      .agg(
        sum("question_entries_count_for_day_and_tag").as("question_entries_count_for_aggregation_interval_and_tag"),
        sum("answer_entries_count_for_day_and_tag").as("answer_entries_count_for_aggregation_interval_and_tag"),
        sum("comment_entries_count_for_day_and_tag").as("comment_entries_count_for_aggregation_interval_and_tag"),
        sum("vote_entries_count_for_day_and_tag").as("vote_entries_count_for_aggregation_interval_and_tag"),
        sum("post_history_entries_count_for_day_and_tag").as("post_history_entries_count_for_aggregation_interval_and_tag"),
        sum("post_link_entries_count_for_day_and_tag").as("post_link_entries_count_for_aggregation_interval_and_tag")
      )
    //    entriesCountByAggregationIntervalAndTag.orderBy($"aggregation_interval".asc).show() //TODO: remove when implementation is finished

    logger.warn("Aggregating entries by aggregation interval.")
    val entriesCountByAggregationInterval = entriesCountByAggregationIntervalAndTag
      .groupBy("aggregation_interval")
      .agg(
        sum($"question_entries_count_for_aggregation_interval_and_tag")
          .as("q__entries_count_for_tag"),
        sum($"question_entries_count_for_aggregation_interval_and_tag" +
          $"answer_entries_count_for_aggregation_interval_and_tag")
          .as("q_a__entries_count_for_tag"),
        sum($"question_entries_count_for_aggregation_interval_and_tag" +
          $"answer_entries_count_for_aggregation_interval_and_tag" +
          $"comment_entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c__entries_count_for_tag"),
        sum($"question_entries_count_for_aggregation_interval_and_tag" +
          $"answer_entries_count_for_aggregation_interval_and_tag" +
          $"comment_entries_count_for_aggregation_interval_and_tag" +
          $"vote_entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c_v__entries_count_for_tag"),
        sum($"question_entries_count_for_aggregation_interval_and_tag" +
          $"answer_entries_count_for_aggregation_interval_and_tag" +
          $"comment_entries_count_for_aggregation_interval_and_tag" +
          $"vote_entries_count_for_aggregation_interval_and_tag" +
          $"post_history_entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c_v_ph__entries_count_for_tag"),
        sum($"question_entries_count_for_aggregation_interval_and_tag" +
          $"answer_entries_count_for_aggregation_interval_and_tag" +
          $"comment_entries_count_for_aggregation_interval_and_tag" +
          $"vote_entries_count_for_aggregation_interval_and_tag" +
          $"post_history_entries_count_for_aggregation_interval_and_tag" +
          $"post_link_entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c_v_ph_pl__entries_count_for_tag")
      )
    //    entriesCountByAggregationInterval.orderBy($"aggregation_interval".asc).show() //TODO: remove when implementation is finished

    logger.warn("ANALYZING: {posts:questions | posts:answers | comments | votes | post_history | post_links}.")
    val entriesCount = entriesCountByAggregationIntervalAndTag
      .join(entriesCountByAggregationInterval, "aggregation_interval")
    //    entriesCount.show() //TODO: remove when implementation is finished
    val relativePopularityByAggregationIntervalAndTag = entriesCount
      .withColumn("q__share",
        $"question_entries_count_for_aggregation_interval_and_tag" / $"q__entries_count_for_tag")
      .withColumn("q_a__share",
        ($"question_entries_count_for_aggregation_interval_and_tag" +
          $"answer_entries_count_for_aggregation_interval_and_tag") / $"q_a__entries_count_for_tag")
      .withColumn("q_a_c__share",
        ($"question_entries_count_for_aggregation_interval_and_tag" +
          $"answer_entries_count_for_aggregation_interval_and_tag" +
          $"comment_entries_count_for_aggregation_interval_and_tag") / $"q_a_c__entries_count_for_tag")
      .withColumn("q_a_c_v__share",
        ($"question_entries_count_for_aggregation_interval_and_tag" +
          $"answer_entries_count_for_aggregation_interval_and_tag" +
          $"comment_entries_count_for_aggregation_interval_and_tag" +
          $"vote_entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v__entries_count_for_tag")
      .withColumn("q_a_c_v_ph__share",
        ($"question_entries_count_for_aggregation_interval_and_tag" +
          $"answer_entries_count_for_aggregation_interval_and_tag" +
          $"comment_entries_count_for_aggregation_interval_and_tag" +
          $"vote_entries_count_for_aggregation_interval_and_tag" +
          $"post_history_entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v_ph__entries_count_for_tag")
      .withColumn("q_a_c_v_ph_pl__share",
        ($"question_entries_count_for_aggregation_interval_and_tag" +
          $"answer_entries_count_for_aggregation_interval_and_tag" +
          $"comment_entries_count_for_aggregation_interval_and_tag" +
          $"vote_entries_count_for_aggregation_interval_and_tag" +
          $"post_history_entries_count_for_aggregation_interval_and_tag" +
          $"post_link_entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v_ph_pl__entries_count_for_tag")
      .withColumn("start", to_date($"aggregation_interval.start"))
      .withColumn("end", to_date($"aggregation_interval.end"))
      .drop("aggregation_interval")
    //    relativePopularityByAggregationIntervalAndTag.show() //TODO: remove when implementation is finished

    logger.warn("Dumping relative popularity results.")
    relativePopularityByAggregationIntervalAndTag
      .select(
        $"start",
        $"end",
        $"tag",
        $"question_entries_count_for_aggregation_interval_and_tag",
        $"answer_entries_count_for_aggregation_interval_and_tag",
        $"comment_entries_count_for_aggregation_interval_and_tag",
        $"vote_entries_count_for_aggregation_interval_and_tag",
        $"post_history_entries_count_for_aggregation_interval_and_tag",
        $"post_link_entries_count_for_aggregation_interval_and_tag",
        $"q__entries_count_for_tag",
        $"q_a__entries_count_for_tag",
        $"q_a_c__entries_count_for_tag",
        $"q_a_c_v__entries_count_for_tag",
        $"q_a_c_v_ph__entries_count_for_tag",
        $"q_a_c_v_ph_pl__entries_count_for_tag",
        $"q__share",
        $"q_a__share",
        $"q_a_c__share",
        $"q_a_c_v__share",
        $"q_a_c_v_ph__share",
        $"q_a_c_v_ph_pl__share",
      )
      .orderBy($"start".asc, $"tag".asc)
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .partitionBy("tag")
      .mode(SaveMode.Append)
      .save(pathToOutput)

    logger.warn("Dumping tags by entries count.")
    tags
      .orderBy($"count".desc)
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Append)
      .save(pathToOutput)

    logger.warn("Done. Exiting.")
  }
}
