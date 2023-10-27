package pl.epsilondeltalimit.algosedd.analyze

import org.apache.spark.sql.DataFrame
import pl.epsilondeltalimit.algosedd.Logging
import pl.epsilondeltalimit.dep.Transformations.PutTransformationWithImplicitCatalog
import pl.epsilondeltalimit.dep.{Catalog, Dep}

object EntriesCount extends PutTransformationWithImplicitCatalog with Logging {

  import Dep.implicits._

  override def apply(implicit c: Catalog) =
    "entriesCountByAggregationIntervalAndTag".as[DataFrame].map2("entriesCountByAggregationInterval".as[DataFrame]) { (entriesCountByAggregationIntervalAndTag, entriesCountByAggregationInterval) =>
      logger.warn("ANALYZING: {posts:questions | posts:answers | comments | votes | post_history | post_links}.")
      entriesCountByAggregationIntervalAndTag
        .join(entriesCountByAggregationInterval, "aggregation_interval")
    }
      .as("entriesCount")
}
