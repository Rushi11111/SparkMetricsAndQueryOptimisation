import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class LimitQuery {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .set("spark.metrics.appStatusSource.enabled","true");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .appName("MySparkApp")
                .master("spark://Rushi-Rajpara:7077")   //use local[*] if you have not set up any cluster
                .getOrCreate();

        System.out.println("AppID : " + spark.sparkContext().getConf().getAppId());

//    -------------------------------------------File With Bigger Row Groups-----------------------------------
//        Dataset<Row> dataset = spark.read().parquet("s3a://learningsparkrr/parqZomato");
//        dataset.limit(1).show();
//    ---------------------------------------------------------------------------------------------------------

//    -------------------------------------------File With Smaller Row Groups----------------------------------
        Dataset<Row> dataset = spark.read().parquet("s3a://learningsparkrr/parqZomatoSP");
        dataset.limit(1).show();
//    ---------------------------------------------------------------------------------------------------------

    }
}
