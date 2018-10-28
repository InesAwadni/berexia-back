import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ReadCsvFiles {
  case class Employee(empno:String, ename:String, designation:String, manager:String, hire_date:String, sal:String , deptno:String)
    var conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val empDF= sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .load("src/main/resources/hackathon.csv")
      .toDF()

    val selectId = empDF.select("Life_ID")
    selectId.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("src/main/resources/f.csv")
    selectId.show()
  }

