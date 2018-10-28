import com.google.gson.Gson;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.codehaus.jackson.map.ObjectMapper;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;

import static spark.Spark.*;

public class Main {
    private static ObjectMapper om = new ObjectMapper();

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Read CSV File").setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        // Start embedded server at this port
        options("/*",
                (request, response) -> {

                    String accessControlRequestHeaders = request
                            .headers("Access-Control-Request-Headers");
                    if (accessControlRequestHeaders != null) {
                        response.header("Access-Control-Allow-Origin",
                                accessControlRequestHeaders);
                    }

                    String accessControlRequestMethod = request
                            .headers("Access-Control-Request-Method");
                    if (accessControlRequestMethod != null) {
                        response.header("Access-Control-Allow-Methods",
                                accessControlRequestMethod);
                    }

                    return "OK";
                });

        before((request, response) -> response.header("Access-Control-Allow-Origin", "*"));
        port(9999);
        post("/api/select", (request, response) -> {
            JSONParser parser = new JSONParser();
            Object obj = parser.parse(request.body());
            JSONObject result = (JSONObject)obj;
            String fileName = result.get("file").toString();
            JSONArray column = (JSONArray) result.get("columns");
            String operation = result.get("operations").toString();

            Dataset<Row> empDF = sqlContext.read().format("com.databricks.spark.csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("delimiter", ";")
                    .load("src/main/resources/"+fileName)
                    .toDF();

            ArrayList list = new ArrayList();
            for (int i = 0; i < column.size(); i++) {
                list.add(new Column(String.valueOf((column.get(i)))));
            }

             Dataset select = empDF.select(convertListToSeq(list)).filter(operation);
            select.show();
            response.status(200);
            return select.collectAsList();
        });
        // Main Page, welcome
        get("/", (request, response) -> {
            Dataset<Row> empDF = sqlContext.read().format("com.databricks.spark.csv")
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("delimiter", ";")
                    .load("src/main/resources/hackathon.csv")
                    .toDF();
            Column selectId1 = empDF.col("Life_ID");
            response.type("application/json");
            response.status(200);
            System.out.print("**********************************************************");
            //empDF.toJSON().foreach(x -> System.out.println(x) );
            response.type("application/json");response.type("application/json");
            response.status(200);
            return new Gson().toJson((new Gson().toJsonTree(empDF.toJSON())));
        });
        //join csv files

    }

    public static Seq<Column> convertListToSeq(List<Column> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

}
