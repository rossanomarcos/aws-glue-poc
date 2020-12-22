
//#########################################################
// IMPORT LIBS AND SET VARIABLES
//#########################################################

// Import glue modules and Spark
import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._

object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)

    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)

    // Parameters
    val glue_db = "poc-glue-db"
    val glue_tbl = "poc_s3_storecsvdata"
    val s3_write_path = "s3://poc-s3-transformedfiles"

    // Initialize contexts and session
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    val dynamic_frame_read = glueContext.getCatalogSource(
                        database = "poc-glue-db",
                        tableName = "poc_s3_storecsvdata",
                        redshiftTmpDir = "",
                        transformationContext = "dynamic_frame_read").getDynamicFrame()

    //#########################################################
    // TRANSFORMATION (MODIFY DATA)
    //#########################################################

    // apply cleaning
    val applymapping1 = dynamic_frame_read.applyMapping(mappings = Seq(("dispatching_base_num", "string", "dispatching_base_num", "string"), ("pickup_date", "string", "pickup_date", "string"), ("locationid", "string", "locationid", "string")), caseSensitive = false, transformationContext = "applymapping1")

    val selectfields2 = applymapping1.selectFields(paths = Seq("dispatching_base_num", "pickup_date", "locationid"), transformationContext = "selectfields2")

    val resolvechoice3 = selectfields2.resolveChoice(choiceOption = Some(ChoiceOption("MATCH_CATALOG")), database = Some("poc-glue-db"), tableName = Some("poc_s3_storecsvdata"), transformationContext = "resolvechoice3")

    //#########################################################
    // LOAD (WRITE DATA)
    //#########################################################

   val datasink4 = glueContext.getSinkWithFormat(
                    connectionType = "s3",
                    options = JsonOptions("""{"path": "s3://poc-s3-transformedfiles"}"""),
                    transformationContext = "datasink4", format = "parquet").writeDynamicFrame(resolvechoice3)

    Job.commit()
  }
}