package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.Transformations.Transformation
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object EntriesCount extends Transformation with Logging {
  override def apply(c: Catalog): Catalog =
    c.put {
      Dep.map2("entriesCount")(
        c.get[DataFrame]("entriesCountByAggregationIntervalAndTag"),
        c.get[DataFrame]("entriesCountByAggregationInterval")
      ) { (entriesCountByAggregationIntervalAndTag, entriesCountByAggregationInterval) =>
        logger.warn("ANALYZING: {posts:questions | posts:answers | comments | votes | post_history | post_links}.")
        entriesCountByAggregationIntervalAndTag
          .join(entriesCountByAggregationInterval, "aggregation_interval")
      }
    }
}
