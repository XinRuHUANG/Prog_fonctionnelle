package processing

import org.apache.spark.sql.Dataset
import model.PollutionRecord

object DataCleaning {
  def clean(ds: Dataset[PollutionRecord]): Dataset[PollutionRecord] = {
    import ds.sparkSession.implicits._   // ← indispensable

    ds.dropDuplicates()
      .na.drop()
      .as[PollutionRecord]
  }
}
