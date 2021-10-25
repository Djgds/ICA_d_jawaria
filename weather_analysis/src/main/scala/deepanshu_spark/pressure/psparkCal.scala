package deepanshu_spark.pressure
import deepanshu_spark.helper.SparkConfiguration
import org.apache.log4j.Logger
import org.apache.spark.sql.{SaveMode}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{StringType, StructType}
import java.io.FileNotFoundException

object psparkCal extends SparkConfiguration {

  // logger
  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {

    // Naming spark program
    spark.conf.set("spark.app.name", " transforming pressure data  ")
    log.info("pressure transformation started")
    Cleanup()
    stopSpark()
  }

  def Cleanup(): Unit = {
    val const="NaN"
    try {
      val defaultSchema = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("p_mor", StringType)
        .add("p_aft", StringType)
        .add("pre_eve", StringType)

      val pressureDf = spark.read.schema(defaultSchema).textFile("pressure.manual.input.dir")

      val mPressureSchemaDF = pressureDf
        .withColumn("stn", lit("manual"))
        .withColumn("p_unit", lit("hpa"))
        .withColumn("baro_reading_1",  lit(const))
        .withColumn("baro_reading_2",  lit(const))
        .withColumn("baro_reading_3",  lit(const))
        .withColumn("therm_reading_1",  lit(const))
        .withColumn("therm_reading_2",  lit(const))
        .withColumn("therm_reading_3",  lit(const))
        .withColumn("reduced_to_1",  lit(const))
        .withColumn("reduced_to_2",  lit(const))
        .withColumn("reduced_to_3",  lit(const))

      val pressureDataDF = mPressureSchemaDF.select(
        "year", "month", "day", "p_mor", "p_aft", "p_eve",
        "stn", "p_unit", "baro_reading_1", "baro_reading_2",
        "baro_reading_3", "therm_reading_1",
        "therm_reading_2", "therm_reading_3",
        "reduced_to_1", "reduced_to_2", "reduced_to_3")

      /** automatic presssure data */
      val aPressureDf = spark.read.schema(defaultSchema).textFile("pressure.automatic.input.dir")

      val aPressureSchemaDF = aPressureDf
        .withColumn("stn", lit("Automatic"))
        .withColumn("p_unit", lit("hpa"))
        .withColumn("baro_reading_1",  lit(const))
        .withColumn("baro_reading_2",  lit(const))
        .withColumn("baro_reading_3",  lit(const))
        .withColumn("therm_reading_1",  lit(const))
        .withColumn("therm_reading_2",  lit(const))
        .withColumn("therm_reading_3",  lit(const))
        .withColumn("reduced_to_1",  lit(const))
        .withColumn("reduced_to_2",  lit(const))
        .withColumn("reduced_to_3",  lit(const))

      val aPressureDataDF = aPressureSchemaDF.select(
        "year", "month", "day", "p_mor", "p_aft", "p_eve",
        "stn", "p_unit", "baro_reading_1", "baro_reading_2",
        "baro_reading_3", "therm_reading_1",
        "therm_reading_2", "therm_reading_3",
        "reduced_to_1", "reduced_to_2", "reduced_to_3")

      //    Cleaned  Pressure Data
      val schema1938 = new StructType()
        .add("extra", StringType)
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("p_mor", StringType)
        .add("p_aft", StringType)
        .add("p_eve", StringType)

      val pressure1938Df = spark.read.schema(schema1938).textFile("pressure.1938.input.dir")

      val pressureTemp1938SchemaDF = pressure1938Df
        .drop("extra")
        .withColumn("stn", lit(const))
        .withColumn("p_unit", lit("hpa"))
        .withColumn("baro_reading_1",  lit(const))
        .withColumn("baro_reading_2",  lit(const))
        .withColumn("baro_reading_3",  lit(const))
        .withColumn("therm_reading_1",  lit(const))
        .withColumn("therm_reading_2",  lit(const))
        .withColumn("therm_reading_3",  lit(const))
        .withColumn("reduced_to_1",  lit(const))
        .withColumn("reduced_to_2",  lit(const))
        .withColumn("reduced_to_3",  lit(const))

      val pressureFinal1938DF = pressureTemp1938SchemaDF.select(
        "year", "month", "day", "p_mor", "p_aft", "p_eve",
        "stn", "p_unit", "baro_reading_1", "baro_reading_2",
        "baro_reading_3", "therm_reading_1",
        "therm_reading_2", "therm_reading_3",
        "reduced_to_1", "reduced_to_2", "reduced_to_3")

      // Cleaned  Pressure Data (1862_mmhg)
      val schema1862 = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("p_mor", StringType)
        .add("p_aft", StringType)
        .add("p_eve", StringType)

      val pressureRaw1862DF = spark.read.schema(schema1862).textFile("pressure.1862.input.dir")

      val pressure1862SchemaDF = pressureRaw1862DF
        .drop("extra")
        .withColumn("stn", lit(const))
        .withColumn("p_unit", lit("mmhg"))
        .withColumn("baro_reading_1",  lit(const))
        .withColumn("baro_reading_2",  lit(const))
        .withColumn("baro_reading_3",  lit(const))
        .withColumn("therm_reading_1",  lit(const))
        .withColumn("therm_reading_2",  lit(const))
        .withColumn("therm_reading_3",  lit(const))
        .withColumn("reduced_to_0",  lit(const))
        .withColumn("reduced_to_1",  lit(const))
        .withColumn("reduced_to_2",  lit(const))

      val pressureFinal1862DF = pressure1862SchemaDF.select(
        "year", "month", "day", "p_mor", "p_aft", "p_eve",
        "stn", "p_unit", "baro_reading_1", "baro_reading_2",
        "baro_reading_3", "therm_reading_1",
        "therm_reading_2", "therm_reading_3",
        "reduced_to_1", "reduced_to_2", "reduced_to_3")

      //  Pressure Data (1756)
      val Schema1756 = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("p_mor", StringType)
        .add("baro_reading_1", StringType)
        .add("p_aft", StringType)
        .add("baro_reading_2", StringType)
        .add("p_eve", StringType)
        .add("baro_reading_3", StringType)

      val pressureData1756TempDF = spark.read.schema(Schema1756).textFile("pressure.1756.input.dir")
      val pressureData1756SchemaDF = pressureData1756TempDF
        .withColumn("stn", lit(const))
        .withColumn("p_unit", lit("Swedish inches (29.69 mm)"))
        .withColumn("therm_reading_1",  lit(const))
        .withColumn("therm_reading_2",  lit(const))
        .withColumn("therm_reading_3",  lit(const))
        .withColumn("reduced_to_1",  lit(const))
        .withColumn("reduced_to_2", lit(const))
        .withColumn("reduced_to_3",  lit(const))

      val pressureFinal1756DF = pressureData1756SchemaDF.select(
        "year", "month", "day", "p_mor", "p_aft", "p_eve",
        "stn", "p_unit", "baro_reading_1", "baro_reading_2",
        "baro_reading_3", "therm_reading_1",
        "therm_reading_2", "therm_reading_3",
        "reduced_to_1", "reduced_to_2", "reduced_to_3")

      /** pressure data  1859 */
      val Schema1859 = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("p_mor", StringType)
        .add("therm_reading_1", StringType)
        .add("reduced_to_1", StringType)
        .add("pressure_noon", StringType)
        .add("therm_reading_2", StringType)
        .add("reduced_to_2", StringType)
        .add("pressure_evening", StringType)
        .add("therm_reading_3", StringType)
        .add("reduced_to_3", StringType)

      val pressureRaw1859Df = spark.read.schema(Schema1859).textFile("presssure.1859.input.dir")

      val pressure1859SchemaDF = pressureRaw1859Df
        .withColumn("stn",  lit(const))
        .withColumn("p_unit", lit("0.1*Swedish inches (2.969 mm)"))
        .withColumn("baro_reading_1",  lit(const))
        .withColumn("baro_reading_2",  lit(const))
        .withColumn("baro_reading_3",  lit(const))

      val pressureFinal1859DF = pressure1859SchemaDF.select(
        "year", "month", "day", "p_mor", "p_aft", "p_eve",
        "stn", "p_unit", "baro_reading_1", "baro_reading_2",
        "baro_reading_3", "therm_reading_1",
        "therm_reading_2", "therm_reading_3",
        "reduced_to_1", "reduced_to_2", "reduced_to_3")

      //                Pressure Data (1961)
      val schema1961 = new StructType()
        .add("year", StringType)
        .add("month", StringType)
        .add("day", StringType)
        .add("p_mor", StringType)
        .add("p_aft", StringType)
        .add("p_eve", StringType)

      val pressureRaw1961Df = spark.read.schema(schema1961).textFile("presssure.1961.input.dir")

      val pressure1961SchemaDF = pressureRaw1961Df
        .withColumn("stn",  lit(const))
        .withColumn("p_unit", lit("hpa"))
        .withColumn("baro_raeding_1",  lit(const))
        .withColumn("baro_raeding_2",  lit(const))
        .withColumn("baro_raeding_3",  lit(const))
        .withColumn("therm_reading_1",  lit(const))
        .withColumn("therm_reading_2", lit(const))
        .withColumn("therm_reading_3",  lit(const))
        .withColumn("reduced_to_1",  lit(const))
        .withColumn("reduced_to_2",  lit(const))
        .withColumn("reduced_to_3", lit(const))

      val pressureFinal1961DF = pressure1961SchemaDF.select(
        "year", "month", "day", "p_mor", "p_aft", "p_eve",
        "stn", "p_unit", "baro_reading_1", "baro_reading_2",
        "baro_reading_3", "therm_reading_1",
        "therm_reading_2", "therm_reading_3",
        "reduced_to_1", "reduced_to_2", "reduced_to_3")

      // Final transformed pressure data
      val pressureDF = pressureDataDF
        .union(aPressureDataDF)
        .union(pressureFinal1938DF)
        .union(pressureFinal1862DF)
        .union(pressureFinal1756DF)
        .union(pressureFinal1859DF)
        .union(pressureFinal1961DF)

      /** saving to hive table */
      import spark.sql
      sql(
        """CREATE TABLE PresData(
        year String,
        month String,
        day String,
        p_mor String,
        p_aft String,
        p_eve String,
        stn String,
        p_unit String,
        baro_reading_1 String,
        baro_reading_2 String,
        baro_reading_3 String,
        therm_reading_1 String,
        therm_reading_2 String,
        therm_reading_3 String,
        reduced_to_1 String,
        reduced_to_2 String,
        reduced_to_3 String)
      STORED AS PARQUET""")
      pressureDF.write.mode(SaveMode.Overwrite).saveAsTable("PresData")

      /** data reconcilation */
      val totalInputCount = pressureDataDF.count() +
        aPressureDataDF.count() +
        pressureFinal1938DF.count() +
        pressureFinal1862DF.count() +
        pressureFinal1756DF.count() +
        pressureFinal1859DF.count() +
        pressureFinal1961DF.count()

      log.info("Input data count is " + totalInputCount)
      log.info("Transformed input data count is " + pressureDF.count())
      log.info("Hive data count " + sql("SELECT count(*) as count FROM PresData").show(false))

    } catch {
      case fileNotFoundException: FileNotFoundException => {
        log.error("Input file not found")

      }
      case exception: Exception => {
        log.error("Exception found " + exception)

      }
    }
  }
}
