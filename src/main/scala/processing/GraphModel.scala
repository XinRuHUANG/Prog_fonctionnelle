package processing

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object GraphModel {

  def buildGraph(df: DataFrame): Graph[String, String] = {

    val spark = df.sparkSession
    import spark.implicits._

    val stations = df.select("Station").distinct().as[String].collect().zipWithIndex
    val stationToId = stations.toMap

    val vertices: RDD[(VertexId, String)] =
      spark.sparkContext.parallelize(
        stations.map { case (name, id) => (id.toLong, name) }
      )

    val stationLigne = df
      .select("Station", "Ligne de transport")
      .distinct()
      .as[(String, String)]
      .collect()

    val edgesSeq =
      for {
        (s1, l1) <- stationLigne
        (s2, l2) <- stationLigne
        if l1 == l2 && s1 != s2
      } yield Edge(stationToId(s1).toLong, stationToId(s2).toLong, l1)

    val edges: RDD[Edge[String]] =
      spark.sparkContext.parallelize(edgesSeq)

    Graph(vertices, edges)
  }
}
