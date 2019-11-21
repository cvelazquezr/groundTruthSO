package text_classification
import spark_conf.Context

object DataConstruction extends Context {
    def main (args: Array[String]): Unit =  {
        println("Loading data from mysql table ...")

        val data = sparkSession.sqlContext.read.format("jdbc").options(
            Map(
                "url" -> "jdbc:mysql://localhost:3306/ground_truth",
                "driver" -> "com.mysql.cj.jdbc.Driver",
                "dbtable" -> "data",
                "user" -> "kmilo",
                "password" -> "kmilo123"
            )).load()
        
        println(data.count())
        data.show()

        sparkSession.close()
    }
}