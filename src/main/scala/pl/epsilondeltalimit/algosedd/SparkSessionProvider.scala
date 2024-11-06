package pl.epsilondeltalimit.algosedd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import pl.epsilondeltalimit.dep.catalog.Catalog

object SparkSessionProvider extends (Catalog => Catalog) {
  override def apply(c: Catalog): Catalog =
    c.put("spark") {
      SparkSession
        .builder()
        .appName(AlgoSEDD.getClass.getSimpleName)
        .config(new SparkConf())
        .getOrCreate()
    }
}
