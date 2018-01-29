import info.bliki.wiki.model.WikiModel
import org.apache.spark.sql.SparkSession
import org.jsoup.Jsoup

/**
  * Created by Nam on 1/29/2018.
  */
object BlikiParser {
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

    val df = sqlContext.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "page")
      //      .load("F:\\input_wiki\\viwiki-20180101-pages-meta-current.xml")
      .load("input.xml")
    df.printSchema()
    df.rdd.map(t=>{
      val wikitext = t.getStruct(4).getStruct(8).getString(0)
      if(wikitext == null){
        println("NYLLLLLLLLL"+t.getLong(0))
      }
      val html = WikiModel.toHtml(wikitext)
      println(Jsoup.parse(html).text())

    })
      .count()
  }
}
