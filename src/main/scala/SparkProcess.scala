import info.bliki.wiki.model.WikiModel
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.jsoup.Jsoup
import utils.EscapseUtils

/**
  * Created by Nam on 1/29/2018.
  * root
  * |-- id: long (nullable = true)
  * |-- ns: long (nullable = true)
  * |-- redirect: struct (nullable = true)
  * |    |-- _VALUE: string (nullable = true)
  * |    |-- _title: string (nullable = true)
  * |-- restrictions: string (nullable = true)
  * |-- revision: struct (nullable = true)
  * |    |-- comment: struct (nullable = true)
  * |    |    |-- _VALUE: string (nullable = true)
  * |    |    |-- _deleted: string (nullable = true)
  * |    |-- contributor: struct (nullable = true)
  * |    |    |-- _VALUE: string (nullable = true)
  * |    |    |-- _deleted: string (nullable = true)
  * |    |    |-- id: long (nullable = true)
  * |    |    |-- ip: string (nullable = true)
  * |    |    |-- username: string (nullable = true)
  * |    |-- format: string (nullable = true)
  * |    |-- id: long (nullable = true)
  * |    |-- minor: string (nullable = true)
  * |    |-- model: string (nullable = true)
  * |    |-- parentid: long (nullable = true)
  * |    |-- sha1: string (nullable = true)
  * |    |-- text: struct (nullable = true)
  * |    |    |-- _VALUE: string (nullable = true)
  * |    |    |-- _space: string (nullable = true)
  * |    |-- timestamp: string (nullable = true)
  * |-- title: string (nullable = true)
  */
object SparkProcess {

  case class ViwikiText(_VALUE: String, _space: String)

  case class ViwikiContributor(id: Long, ip: String, username: String)

  case class ViwikiRevision(comment: String, contributor: ViwikiContributor, format: String, id: Long, minor: String, model: String, parentid: Long, sha1: String, text: ViwikiText, timestamp: String)

  case class ViwikiRecord(id: Long, ns: Long, restrictions: String, revision: ViwikiRevision, title: String)

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil")
    import org.apache.spark.sql.SQLContext
    import com.databricks.spark.xml._
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("spark session example")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    import spark.implicits._
    spark.read.parquet("F:\\input_wiki\\parquet")
      .show(10)
    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      .load("F:\\input_wiki\\viwiki-20180101-pages-meta-current.xml")
//      .load("input.xml")
      .limit(10000)

    df.printSchema()

    df.rdd.map(row => {
      var id: Long = -1
      var ns: Long = -1
      var restrictions: String = null
      var title: String = null
      var format: String = ViwikiTextCleaner.FORMAT_TEXT_PLAIN
      var r_id: Long = -1
      var minor: String = null
      var model: String = null
      var parentid: Long = -1
      var sha1: String = null
      var timestamp: String = null
      var comment: Row = null
      var contributor: Row = null
      if (!row.isNullAt(0)) id = row.getLong(0)
      if (!row.isNullAt(1)) ns = row.getLong(1)
      if (!row.isNullAt(3)) restrictions = row.getString(3)
      val revision = row.getStruct(4)
      if (!row.isNullAt(5)) title = row.getString(5)
      if (!revision.isNullAt(0)) comment = revision.getStruct(0)
      if (!revision.isNullAt(1)) contributor = revision.getStruct(1)
//      if (!revision.isNullAt(2)) format = revision.getString(2)
      if (!revision.isNullAt(3)) r_id = revision.getLong(3)
      if (!revision.isNullAt(4)) minor = revision.getString(4)
      if (!revision.isNullAt(5)) model = revision.getString(5)
      if (!revision.isNullAt(6)) parentid = revision.getLong(6)
      if (!revision.isNullAt(7)) sha1 = revision.getString(7)
      if (!revision.isNullAt(9)) timestamp = revision.getString(9)
      var text: ViwikiText = null
      if (!revision.isNullAt(8)) {
        val textStruct = revision.getStruct(8)
        if(!textStruct.isNullAt(0)) {
//          val html = WikiModel.toHtml(textStruct.getString(0))
//          println(Jsoup.parse(html).text())
          text = ViwikiText(ViwikiTextCleaner.clean(textStruct.getString(0)), "preserve")
        }
      }
      if(text == null) text = ViwikiText(null, "preserve")
      var _comment:String = null
      var _contributor:ViwikiContributor = null
      if(comment!=null){
        if(!comment.isNullAt(0)) _comment = comment.getString(0)
      }
      if(contributor != null){
        var c_id: Long = -1
        var c_ip: String = null
        var c_username: String = null
        if(!contributor.isNullAt(2)) c_id = contributor.getLong(2)
        if(!contributor.isNullAt(3)) c_ip = contributor.getString(3)
        if(!contributor.isNullAt(4)) c_username = contributor.getString(4)
        _contributor = ViwikiContributor(c_id,c_ip,c_username)
      }
      val _revision = ViwikiRevision(_comment, _contributor, format, r_id, minor, model, parentid, sha1, text, timestamp)

      //      val _revision = ViwikiRevision(null, null, format, r_id, minor, model, parentid, sha1, text, timestamp)
      ViwikiRecord(id, ns, restrictions, _revision, title)
      //      title
    })
      //        .repartition(1)
      //      .saveAsTextFile("output/")
      .toDF()
      .repartition(1)
      .write
      .mode("overwrite")
      .parquet("F:\\input_wiki\\parquet")
    //
    ////      .count()


  }
}
