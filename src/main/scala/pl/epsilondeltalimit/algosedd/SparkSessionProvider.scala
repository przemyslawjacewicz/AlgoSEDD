package pl.epsilondeltalimit.algosedd

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import pl.epsilondeltalimit.dep.v6_1.{Catalog, Transformation}

object SparkSessionProvider extends Transformation {
  override def apply(c: Catalog): Catalog =
    c.unit("spark") {
      SparkSession
        .builder()
        .appName(AlgoSEDD.getClass.getSimpleName)
        .config(new SparkConf())
        .getOrCreate()
    }
}
