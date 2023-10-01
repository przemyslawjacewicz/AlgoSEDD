package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, to_date}
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object RelativePopularityByAggregationIntervalAndTag extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      c.get[DataFrame]("entriesCount").map("relativePopularityByAggregationIntervalAndTag") { entriesCount =>
        entriesCount
          .withColumn(
            "q__share",
            col("q__entries_count_for_aggregation_interval_and_tag") / col("q__entries_count_for_aggregation_interval")
          )
          .withColumn(
            "q_a__share",
            (col("q__entries_count_for_aggregation_interval_and_tag") +
              col("a__entries_count_for_aggregation_interval_and_tag")) / col(
              "q_a__entries_count_for_aggregation_interval")
          )
          .withColumn(
            "q_a_c__share",
            (col("q__entries_count_for_aggregation_interval_and_tag") +
              col("a__entries_count_for_aggregation_interval_and_tag") +
              col("c__entries_count_for_aggregation_interval_and_tag")) / col(
              "q_a_c__entries_count_for_aggregation_interval")
          )
          .withColumn(
            "q_a_c_v__share",
            (col("q__entries_count_for_aggregation_interval_and_tag") +
              col("a__entries_count_for_aggregation_interval_and_tag") +
              col("c__entries_count_for_aggregation_interval_and_tag") +
              col("v__entries_count_for_aggregation_interval_and_tag")) / col(
              "q_a_c_v__entries_count_for_aggregation_interval")
          )
          .withColumn(
            "q_a_c_v_ph__share",
            (col("q__entries_count_for_aggregation_interval_and_tag") +
              col("a__entries_count_for_aggregation_interval_and_tag") +
              col("c__entries_count_for_aggregation_interval_and_tag") +
              col("v__entries_count_for_aggregation_interval_and_tag") +
              col("ph__entries_count_for_aggregation_interval_and_tag")) / col(
              "q_a_c_v_ph__entries_count_for_aggregation_interval")
          )
          .withColumn(
            "q_a_c_v_ph_pl__share",
            (col("q__entries_count_for_aggregation_interval_and_tag") +
              col("a__entries_count_for_aggregation_interval_and_tag") +
              col("c__entries_count_for_aggregation_interval_and_tag") +
              col("v__entries_count_for_aggregation_interval_and_tag") +
              col("ph__entries_count_for_aggregation_interval_and_tag") +
              col("pl__entries_count_for_aggregation_interval_and_tag")) / col(
              "q_a_c_v_ph_pl__entries_count_for_aggregation_interval")
          )
          .withColumn("start", to_date(col("aggregation_interval.start")))
          .withColumn("end", to_date(col("aggregation_interval.end")))
          .drop("aggregation_interval")
      }
    }
}
