package extraction
import spark_conf.Context

import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements

import org.apache.spark.sql.Encoder
import scala.reflect.ClassTag
import java.io.{PrintWriter, File}


object Processing extends Context {
    import sparkSession.sqlContext.implicits._

    def processSnippetCode(snippet: String): Tuple2[String, String] = {
        val lineSplitted: Array[String] = snippet.split("\n")
        
        val importLines: Array[String] = lineSplitted.filter(line => {
            line.trim().contains("import")
        }).map(importLine => {
            val splittedImport: Array[String] = importLine.split(" ")
            if (splittedImport.length == 2)
                splittedImport(1).dropRight(1)
            else ""
        }).filter(_.length() > 0)

        val notImportLines: Array[String] = lineSplitted.filter(line => {
            !line.trim().contains("import")
        }).map(_.trim()).filter(_.length() > 0)

        (notImportLines.mkString(" "), importLines.mkString(" "))
    }

    def processBody(body: String): Map[String, String] = {
        val html: Document = Jsoup.parse(body)
        val codes: Elements = html.select("pre")

        // Each element in the array of codes could be considered as a snippet of code
        val codesArray: Array[String] = codes.toArray().map(code => code.asInstanceOf[Element].text())

        // Process for each snippet
        codesArray.map(snippet => processSnippetCode(snippet)).toMap
    }

    def main (args: Array[String]): Unit = {
        //TODO: Extract the import lines from the snippets
        //TODO: Extract the links from the text around the snippets

        println("Loading the data ...")
        val with_imports_data = sparkSession
            .read
            .option("quote", "\"")
            .option("escape", "\"")
            .option("header", "true")
            .option("inferSchema", "true")
            .csv("data/posts/with_imports")
            .toDF()

        println("Processing the data ...")
        val testElement: String = with_imports_data.head().getAs[String]("Body")
        processBody(testElement)

        val processedBodies = with_imports_data.flatMap(row => {
            val question_ID: Int = row.getAs[Int]("Question_ID")
            val answer_ID: Int = row.getAs[Int]("Answer_ID")

            val body: String = row.getAs[String]("Body")
            val processedBody: Map[String, String] = processBody(body)
            
            val snippets: Seq[Tuple4[Int, Int, String, String]] = processedBody.map(bodyProcessed => {
                (question_ID, answer_ID, bodyProcessed._1, bodyProcessed._2)
            }).toSeq

            snippets
        }).collect().toSeq.toDF("questionID", "answerID", "code", "imports")

        println("Writing the ground truth dataset into mysql ...")
        val prop = new java.util.Properties
        prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
        prop.setProperty("user", "kmilo")
        prop.setProperty("password", "kmilo123")

        // URL for the database
        val url = "jdbc:mysql://localhost:3306/ground_truth"

        // Name of the table
        val table = "data"

        // Writing values to the table
        processedBodies.write.mode("append").jdbc(url, table, prop)

        sparkSession.close()
    }
}