package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.to_date
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object RelativePopularityByAggregationIntervalAndTag extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("entriesCount").map("relativePopularityByAggregationIntervalAndTag") { entriesCount =>
        import entriesCount.sparkSession.implicits._

        entriesCount
          .withColumn(
            "q__share",
            $"q__entries_count_for_aggregation_interval_and_tag" / $"q__entries_count_for_aggregation_interval"
          )
          .withColumn(
            "q_a__share",
            ($"q__entries_count_for_aggregation_interval_and_tag" +
              $"a__entries_count_for_aggregation_interval_and_tag") / $"q_a__entries_count_for_aggregation_interval"
          )
          .withColumn(
            "q_a_c__share",
            ($"q__entries_count_for_aggregation_interval_and_tag" +
              $"a__entries_count_for_aggregation_interval_and_tag" +
              $"c__entries_count_for_aggregation_interval_and_tag") / $"q_a_c__entries_count_for_aggregation_interval"
          )
          .withColumn(
            "q_a_c_v__share",
            ($"q__entries_count_for_aggregation_interval_and_tag" +
              $"a__entries_count_for_aggregation_interval_and_tag" +
              $"c__entries_count_for_aggregation_interval_and_tag" +
              $"v__entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v__entries_count_for_aggregation_interval"
          )
          .withColumn(
            "q_a_c_v_ph__share",
            ($"q__entries_count_for_aggregation_interval_and_tag" +
              $"a__entries_count_for_aggregation_interval_and_tag" +
              $"c__entries_count_for_aggregation_interval_and_tag" +
              $"v__entries_count_for_aggregation_interval_and_tag" +
              $"ph__entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v_ph__entries_count_for_aggregation_interval"
          )
          .withColumn(
            "q_a_c_v_ph_pl__share",
            ($"q__entries_count_for_aggregation_interval_and_tag" +
              $"a__entries_count_for_aggregation_interval_and_tag" +
              $"c__entries_count_for_aggregation_interval_and_tag" +
              $"v__entries_count_for_aggregation_interval_and_tag" +
              $"ph__entries_count_for_aggregation_interval_and_tag" +
              $"pl__entries_count_for_aggregation_interval_and_tag") / $"q_a_c_v_ph_pl__entries_count_for_aggregation_interval"
          )
          .withColumn("start", to_date($"aggregation_interval.start"))
          .withColumn("end", to_date($"aggregation_interval.end"))
          .drop("aggregation_interval")
      }
    }
}
