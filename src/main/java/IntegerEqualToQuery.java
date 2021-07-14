import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IntegerEqualToQuery {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .set("spark.metrics.appStatusSource.enabled", "true")
                .set("parquet.dictionary.page.size","4194304");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .appName("MySparkApp")
                .master("spark://Rushi-Rajpara:7077")   //use local[*] if you have not set up any cluster
                .getOrCreate();

        System.out.println("AppID : " + spark.sparkContext().getConf().getAppId());

        Dataset<Row> dataset = spark.read().parquet("s3a://learningsparkrr/parqTweets");
        dataset.where("likes = 10000").show();

    }
}
