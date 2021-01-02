package pl.epsilondeltalimit.analyzer

import java.sql.Date
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDate}

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{RowFactory, SaveMode, SparkSession}
import pl.epsilondeltalimit.analyzer.analyze.AnalyzeSupport._
import pl.epsilondeltalimit.analyzer.read._

//TODO: customize logging - add start and finish msg
object StackExchangeDataDumpAnalyzerSingle {
  private[this] val logger = Logger.getLogger(StackExchangeDataDumpAnalyzerSingle.getClass.getSimpleName)

  def main(args: Array[String]): Unit = {
    val startDate = args(0)
    val endDate = args(1)
    val aggregationInterval = args(2)
    val pathToBadgesFile = args(3)
    val pathToCommentsFile = args(4)
    val pathToPostHistoryFile = args(5)
    val pathToPostLinksFile = args(6)
    val pathToPostsFile = args(7)
    val pathToTagsFile = args(8)
    val pathToUsersFile = args(9)
    val pathToVotesFile = args(10)
    val pathToOutputRoot = args(11)

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
    val creationDateAndTag = Seq("creation_date", "tag")
    val dataEntriesCountByCreationDateAndTag = countEntriesByCreationDateAndTag(tagsByCreationDataFromQuestions)
      .withColumnRenamed("count", "q__entries_count_for_day_and_tag")
      .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromAnswers), creationDateAndTag)
      .withColumnRenamed("count", "a__entries_count_for_day_and_tag")
      .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromComments), creationDateAndTag)
      .withColumnRenamed("count", "c__entries_count_for_day_and_tag")
      .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromVotes), creationDateAndTag)
      .withColumnRenamed("count", "v__entries_count_for_day_and_tag")
      .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromPostHistory), creationDateAndTag)
      .withColumnRenamed("count", "ph__entries_count_for_day_and_tag")
      .join(countEntriesByCreationDateAndTag(tagsByCreationDataFromPostLinks), creationDateAndTag)
      .withColumnRenamed("count", "pl__entries_count_for_day_and_tag")
    //    dataEntriesCountByCreationDateAndTag.show() //TODO: remove when implementation is finished

    val fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val daysBetween = Duration.between(LocalDate.parse(startDate, fmt).atStartOfDay(), LocalDate.parse(endDate, fmt).atStartOfDay()).toDays
    val days = spark.createDataFrame(
      spark.sparkContext.parallelize(for (day <- 0L to daysBetween) yield RowFactory.create(Date.valueOf(LocalDate.parse(startDate, fmt).plusDays(day)))),
      StructType(Array(StructField("creation_date", DataTypes.DateType))))
    //    val days = (for (day <- 0L to daysBetween) yield LocalDate.parse(startDate, fmt).plusDays(day)).toDF("creation_date")
    val daysAndTags = days
      .crossJoin(tags.select($"tag_name".as("tag")))
    //    daysAndTags.show() //TODO: remove when implementation is finished

    logger.warn("Mapping entries to date range and tags.")
    val entriesCountByCreationDateAndTag = daysAndTags
      .join(dataEntriesCountByCreationDateAndTag, Seq("creation_date", "tag"), "left_outer")
    //    entriesCountByCreationDateAndTag.orderBy($"creation_date".desc).show() //TODO: remove when implementation is finished

    logger.warn("Aggregating entries by aggregation interval and tag.")
    val entriesCountByAggregationIntervalAndTag = entriesCountByCreationDateAndTag
      .groupBy(window($"creation_date", aggregationInterval).as("aggregation_interval"), $"tag")
      .agg(
        sum("q__entries_count_for_day_and_tag").as("q__entries_count_for_aggregation_interval_and_tag"),
        sum("a__entries_count_for_day_and_tag").as("a__entries_count_for_aggregation_interval_and_tag"),
        sum("c__entries_count_for_day_and_tag").as("c__entries_count_for_aggregation_interval_and_tag"),
        sum("v__entries_count_for_day_and_tag").as("v__entries_count_for_aggregation_interval_and_tag"),
        sum("ph__entries_count_for_day_and_tag").as("ph__entries_count_for_aggregation_interval_and_tag"),
        sum("pl__entries_count_for_day_and_tag").as("pl__entries_count_for_aggregation_interval_and_tag")
      )
    //    entriesCountByAggregationIntervalAndTag.orderBy($"aggregation_interval".desc).show() //TODO: remove when implementation is finished

    logger.warn("Aggregating entries by aggregation interval.")
    val entriesCountByAggregationInterval = entriesCountByAggregationIntervalAndTag
      .groupBy("aggregation_interval")
      .agg(
        sum($"q__entries_count_for_aggregation_interval_and_tag")
          .as("q__entries_count_for_aggregation_interval"),
        sum($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag")
          .as("q_a__entries_count_for_aggregation_interval"),
        sum($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c__entries_count_for_aggregation_interval"),
        sum($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c_v__entries_count_for_aggregation_interval"),
        sum($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag" +
          $"ph__entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c_v_ph__entries_count_for_aggregation_interval"),
        sum($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag" +
          $"ph__entries_count_for_aggregation_interval_and_tag" +
          $"pl__entries_count_for_aggregation_interval_and_tag")
          .as("q_a_c_v_ph_pl__entries_count_for_aggregation_interval")
      )
    //    entriesCountByAggregationInterval.orderBy($"aggregation_interval".desc).show() //TODO: remove when implementation is finished

    logger.warn("ANALYZING: {posts:questions | posts:answers | comments | votes | post_history | post_links}.")
    val entriesCount = entriesCountByAggregationIntervalAndTag
      .join(entriesCountByAggregationInterval, "aggregation_interval")
    //    entriesCount.orderBy($"aggregation_interval".desc) show() //TODO: remove when implementation is finished

    val relativePopularityByAggregationIntervalAndTag = entriesCount
      .withColumn("q__share",
        $"q__entries_count_for_aggregation_interval_and_tag" / $"q__entries_count_for_aggregation_interval")
      .withColumn("q_a__share",
        ($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag") / $"q_a__entries_count_for_aggregation_interval")
      .withColumn("q_a_c__share",
        ($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag") / $"q_a_c__entries_count_for_aggregation_interval")
      .withColumn("q_a_c_v__share",
        ($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v__entries_count_for_aggregation_interval")
      .withColumn("q_a_c_v_ph__share",
        ($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag" +
          $"ph__entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v_ph__entries_count_for_aggregation_interval")
      .withColumn("q_a_c_v_ph_pl__share",
        ($"q__entries_count_for_aggregation_interval_and_tag" +
          $"a__entries_count_for_aggregation_interval_and_tag" +
          $"c__entries_count_for_aggregation_interval_and_tag" +
          $"v__entries_count_for_aggregation_interval_and_tag" +
          $"ph__entries_count_for_aggregation_interval_and_tag" +
          $"pl__entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v_ph_pl__entries_count_for_aggregation_interval")
      .withColumn("start", to_date($"aggregation_interval.start"))
      .withColumn("end", to_date($"aggregation_interval.end"))
      .drop("aggregation_interval")
    //    relativePopularityByAggregationIntervalAndTag.orderBy($"aggregation_interval".desc).show() //TODO: remove when implementation is finished

    val formattedAggregationInterval = aggregationInterval.replaceAll(" ", "")
    logger.warn("Dumping relative popularity results.")
    relativePopularityByAggregationIntervalAndTag
      .na
      .fill(0)
      .select(
        $"start",
        $"end",
        $"tag",
        $"q__entries_count_for_aggregation_interval_and_tag",
        $"a__entries_count_for_aggregation_interval_and_tag",
        $"c__entries_count_for_aggregation_interval_and_tag",
        $"v__entries_count_for_aggregation_interval_and_tag",
        $"ph__entries_count_for_aggregation_interval_and_tag",
        $"pl__entries_count_for_aggregation_interval_and_tag",
        $"q__entries_count_for_aggregation_interval",
        $"q_a__entries_count_for_aggregation_interval",
        $"q_a_c__entries_count_for_aggregation_interval",
        $"q_a_c_v__entries_count_for_aggregation_interval",
        $"q_a_c_v_ph__entries_count_for_aggregation_interval",
        $"q_a_c_v_ph_pl__entries_count_for_aggregation_interval",
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
      .save(s"$pathToOutputRoot/$formattedAggregationInterval")

    logger.warn("Dumping tags by entries count.")
    tags
      .orderBy($"count".desc)
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Append)
      .save(s"$pathToOutputRoot/$formattedAggregationInterval")

    logger.warn("Done. Exiting.")
  }
}
