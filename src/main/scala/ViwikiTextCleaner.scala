import java.io._
import java.util.Scanner
import javax.xml.stream.{XMLInputFactory, XMLStreamReader}

import com.mashape.unirest.http.Unirest
import org.jsoup.Jsoup
import parser.XMLParser
import utils.EscapseUtils

/**
  * Created by Nam on 1/28/2018.
  */
object ViwikiTextCleaner {
  val START_TEXT_REGEX = "      <text xml:space=\"preserve\">"
  val END_TEXT_REGEX = "</text>"
  val ID_REGEX = "      <id>"
  val END_POINT = "https://vi.wikipedia.org/w/api.php"

  def clean(wiki_id: Long): String = {
//    println("parsing id: " + wiki_id)
    try {
      val json: String = Unirest.get(END_POINT)
        .queryString("action", "parse")
        .queryString("oldid", wiki_id)
        .queryString("format", "json")
        .asJson()
        .getBody
        .getObject
        .getJSONObject("parse")
        .getJSONObject("text")
        .getString("*")
        .toString
      Jsoup.parse(json).text()
    }catch {
      case e: Exception =>{
        null
      }
    }
  }

  def main(args: Array[String]): Unit = {
  }

}
