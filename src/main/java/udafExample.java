import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Aggregator;

class Concat extends Aggregator<String,String,String> {
    @Override
    public String zero() {
        return "";
    }

    @Override
    public String reduce(String b, String a) {
        return a + b;
    }

    @Override
    public String merge(String b1, String b2) {
        return b1 + b2;
    }

    @Override
    public String finish(String reduction) {
        return reduction;
    }

    @Override
    public Encoder<String> bufferEncoder() {
        return Encoders.STRING();
    }

    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}

public class udafExample {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .set("spark.metrics.appStatusSource.enabled","true");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .appName("MySparkApp")
                .master("spark://Rushi-Rajpara:7077")   //use local[*] if you have not set up any cluster
                .getOrCreate();

        System.out.println("AppID : " + spark.sparkContext().getConf().getAppId());

        Dataset<Row> dataset = spark.read().parquet("s3a://learningsparkrr/parqZomato");

        spark.udf().register("Concat", functions.udaf(new Concat(),Encoders.STRING()));
        dataset.groupBy("name").agg(functions.callUDF("Concat",functions.col("reviews_list"))).show();
    }
}
