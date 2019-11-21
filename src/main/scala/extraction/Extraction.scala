package extraction

import java.io.{File, PrintWriter}

import org.apache.spark.sql.{DataFrame, Encoder}
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document, Element}
import org.jsoup.select.Elements
import spark_conf.Context

import scala.io.Source
import scala.reflect.ClassTag
import org.apache.spark.sql.SaveMode

object Extraction extends Context {
  implicit def kryoEncoder[A](implicit ct: ClassTag[A]): Encoder[A] =
    org.apache.spark.sql.Encoders.kryo[A](ct)

  def cleanText(text: String): String = {
    val cleanedText = text.replaceAll("\n", " ")

    val javaBuffered = Source.fromFile("data/resources/java_words.txt")
    val javaWords: Seq[String] = javaBuffered.getLines().toSeq

    val cleanedWords: Array[String] = cleanedText.split(" ").map(word => word.map(chr => {
      if (chr.isLetter)
        chr
      else
        ' '
    }))

    val cleanEmpty: Array[String] = cleanedWords.mkString(" ").split(" ")
      .filter(word => word.trim.length > 2)
      .map(word => word.trim)

    val filteredClasses: Array[String] = cleanEmpty
      .filter(word => !javaWords.contains(word))

    filteredClasses.mkString(" ")
  }

  def containsImport(body: String): Boolean = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("code")

    val processedCodes: Array[String] = codes
      .toArray()
      .map(_.asInstanceOf[Element].text())
      .mkString(" ")
      .split(" ")

    val importLines = processedCodes.filter(_.contains("import"))

    if (importLines.length > 0)
      return true
    false
  }

  def containsLinks(body: String): Boolean = {
    val html: Document = Jsoup.parse(body)
    val codes: Elements = html.select("a")

    codes.toArray().nonEmpty
  }

  // Regular expressions of the state-of-the-art
  def fullyQualifiedRegex(body: String, typeQualified: String): Int = {
    val expressionRegex = "(?i). ∗\\b"  + typeQualified + "\\b.∗"
    val regex = expressionRegex.r
    regex.findAllIn(body).toArray.length
  }

  def nonQualifiedRegex(body: String, typeName: String): Int = {
    val expressionRegex = ".*(^|[a-z]+ |[\\.!?] |[\\(<])" + typeName + "([>\\)\\.,!?$]| [a-z]+).*"
    val regex = expressionRegex.r
    regex.findAllIn(body).toArray.length
  }

  def linksRegex(body: String, packageName: String, typeName: String): Unit = {
    val expressionRegex = ".∗ <a.∗href.∗" + packageName + "/" + typeName + "\\.html.∗ >.∗ </a>.∗"
    val regex = expressionRegex.r
    regex.findAllIn(body)
  }

  def classMethodsRegex(body: String, className: String, methodName: String): Int = {
    val expressionRegex = ". ∗ "+ className + "\\." + methodName + "[\\(| ]"
    val regex = expressionRegex.r
    regex.findAllIn(body).toArray.length
  }

  def writeToDisk(dataFrame: DataFrame, path: String): Unit = {
    val pw: PrintWriter = new PrintWriter(new File(path))
    dataFrame.collect().foreach(row => {
      val question_ID: Int = row.getAs[Int]("Question_ID")
      val title: String = row.getAs[String]("Title")
      val tags: String = row.getAs[String]("Tags")
      val answer_ID: Int = row.getAs[Int]("Answer_ID")
      val score: Int = row.getAs[Int]("Score")
      val body: String = row.getAs[String]("Body")

      val textToWrite: String = s"$question_ID,$answer_ID,$title,$tags,$body,$score\n"
      pw.write(textToWrite)
    })
    pw.close()
  }

  def main(args: Array[String]): Unit = {
    println("Loading data ...")

    val dataPosts = sparkSession
      .read
      .option("quote", "\"")
      .option("escape", "\"")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/posts/java_posts.csv")
      .toDF()
    val dataNoNull = dataPosts.na.drop()

    val postsWithImports = dataNoNull.filter(row => {
      val body: String = row.getAs[String]("Body")
      containsImport(body)
    })

    val postsWithoutImportsWithLinks = postsWithImports.filter(row => {
      val body: String = row.getAs[String]("Body")
      !containsImport(body) && containsLinks(body)
    })

    println("Saving data ...")
    postsWithImports
    .write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .csv("data/posts/with_imports/")

    postsWithoutImportsWithLinks
    .write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("quote", "\"")
    .option("escape", "\"")
    .csv("data/posts/with_links/")

    sparkSession.stop()
  }
}
