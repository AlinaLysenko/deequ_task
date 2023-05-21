import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.constraints.{ConstrainableDataTypes, ConstraintStatus}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("deequ_task")
      .master("local[*]")
      .getOrCreate()

    val data = spark.read
      .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")  // specify format explicitly
      .option("header", "true")
      .option("delimiter", "\t")
      .option("inferSchema", "true")
      .load("src/main/resources/amazon_reviews_us_Camera_v1_00.tsv")

    val verificationResult = VerificationSuite()
      .onData(data)
      .addCheck(
        Check(CheckLevel.Error, "Data Quality Check")
          .isComplete("review_id")
          .isContainedIn("verified_purchase", Array("Y", "N"))
          .hasPattern("review_date", """\d{4}-\d{2}-\d{2}""".r)
          .isUnique("review_id")
          .hasDataType("total_votes", ConstrainableDataTypes.Integral)
      )
      .run()

    if (verificationResult.status == CheckStatus.Success) {
      println("The data passed the quality checks.")
    } else {
      println("The data did not pass the quality checks, the reasons are:")
      val resultsForAllConstraints = verificationResult.checkResults
        .flatMap { case (_, checkResult) => checkResult.constraintResults }

      resultsForAllConstraints
        .filterNot(_.status == ConstraintStatus.Success)
        .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
    }

    spark.stop()
  }
}
