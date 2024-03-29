package extraction
import spark_conf.Context

import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements

import scala.io.Source


object Processing extends Context {
    import sparkSession.sqlContext.implicits._

    def patternsImports(importsArray: Array[String]): String = {
        // Remove the classes in the import lines
        val removedClasses: Set[String] = importsArray.map(importLine => {
            val splittedLine: Array[String] = importLine.split("\\.")
            val upperCaseWords: Array[String] = splittedLine.filter(word => {
                if (word.length() > 0)
                    word(0).isUpper
                else false
            })

            if (upperCaseWords.length > 0)  {
                val indexUpperCase = splittedLine.indexOf(upperCaseWords(0))
                splittedLine.take(indexUpperCase).mkString(".")
            } else {
                splittedLine.dropRight(1).mkString(".")
            }
        }).toSet

        // Remove the classes with different packages
        var excludedImports: Array[String] = Array()
        removedClasses.foreach(clazz => {
            removedClasses.foreach(clazz2 => {
                if (!clazz.equals(clazz2)) {
                    if (clazz2.startsWith(clazz))
                    excludedImports :+= clazz2
                }
            })
        })

        removedClasses
            .filter(clazz => !excludedImports.contains(clazz))
            .toArray
            .mkString(" ")
    }

    def classesExtraction(code: Array[String]): String = {
        val javaBuffered = Source.fromFile("data/resources/java_words.txt")
        val javaWords: Seq[String] = javaBuffered.getLines().toSeq

        val filteredSigns: Array[String] = code.map(code => {
            val cleanedWords: Array[String] = code.split(" ").map(word => word.map(chr => {
                if (chr.isLetter)
                    chr
                else
                    ' '
            })).filter(_.trim.length > 2)
            cleanedWords.mkString(" ").split(" ").filter(_.trim.length > 2).mkString(" ")
        })
    
        val filteredJava: Array[String] = filteredSigns.map(line => {
            val splittedLine: Array[String] = line.split(" ").filter(word => !javaWords.contains(word.toLowerCase()))

            if (splittedLine.length > 0)
                splittedLine.mkString(" ")
            else ""
        }).filter(_.nonEmpty).mkString(" ").split(" ")

        val filteredClasses: Array[String] = filteredJava.filter(word => {
            if (word.length() > 0)
                word(0).isUpper && !word.endsWith("Exception")
            else false
        })

        filteredClasses.mkString(" ")
    }

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

        (patternsImports(importLines), classesExtraction(notImportLines))
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

        val processedBodies = with_imports_data.flatMap(row => {
            val question_ID: Int = row.getAs[Int]("Question_ID")
            val answer_ID: Int = row.getAs[Int]("Answer_ID")

            val body: String = row.getAs[String]("Body")
            val processedBody: Map[String, String] = processBody(body)
            
            val snippets: Seq[Tuple4[Int, Int, String, String]] = processedBody.map(bodyProcessed => {
                (question_ID, answer_ID, bodyProcessed._1, bodyProcessed._2)
            }).filter(_._3.trim().length() > 0).toSeq

            snippets
        }).collect().toSeq.toDF("questionID", "answerID", "imports", "code")

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