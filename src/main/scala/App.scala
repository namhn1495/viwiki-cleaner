import java.io.{File, FileReader, FileWriter, PrintWriter}
import java.util.Scanner

import ViwikiTextCleaner.{END_TEXT_REGEX, ID_REGEX, START_TEXT_REGEX}
import utils.EscapseUtils

/**
  * Created by Nam on 1/28/2018.
  */
object App {

  import java.io.BufferedInputStream
  import java.io.FileInputStream
  import java.io.IOException
  import java.io.InputStream

  @throws[IOException]
  def countLines(filename: String): Int = {
    val is = new BufferedInputStream(new FileInputStream(filename))
    try {
      val c = new Array[Byte](4096)
      var count = 0
      var readChars = 0
      var empty = true
      while ( {
        (readChars = is.read(c)) != -1
      }) {
        empty = false
        var i = 0
        while ( {
          i < readChars
        }) {
          if (c(i) == '\n') count += 1

          {
            i += 1; i
          }
        }
      }
      if (count == 0 && !(empty)) 1
      else count
    } finally is.close()
  }
  def main(args: Array[String]): Unit = {
    println("Count: "+countLines("F:\\input_wiki\\viwiki-20180101-pages-meta-current.xml"))
    val in = new FileReader("F:\\input_wiki\\viwiki-20180101-pages-meta-current.xml")
    val writer = new PrintWriter(new File("F:\\output_wiki\\wiki-20180101-pages.xml"))
    var id:Long = -1
    import java.io.BufferedReader
    import java.io.FileReader
    val br = new BufferedReader(in)
    var line = ""
    while ((line = br.readLine) != null) {
      if(line!=null) {
        if (line.startsWith(START_TEXT_REGEX)) {
          //        println("=========START=============")
          if (!line.endsWith(END_TEXT_REGEX)) {
            var new_line = ""
            do {
              new_line = br.readLine
            } while (!new_line.endsWith(END_TEXT_REGEX))
          }
          //        println("==========END===========")
          try {
            val new_line = EscapseUtils.escapseXML(ViwikiTextCleaner.clean(id))
            writer.println(START_TEXT_REGEX + new_line + END_TEXT_REGEX)
          }catch {
            case e: Exception => {
              writer.println(START_TEXT_REGEX + END_TEXT_REGEX)
            }
          }
          println("dumped id: " + id)

        } else {
          if (line.startsWith(ID_REGEX)) {
            id = line.replaceAll(ID_REGEX, "").replaceAll("</id>", "").toLong
          }
          writer.println(line)
        }
      }else{
        writer.close()
        return
      }


    }
    writer.close()

  }
}
