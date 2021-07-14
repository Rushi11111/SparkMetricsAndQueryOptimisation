import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class JoinTable {
    // Comparing broadcast join and sort merge join types of joining mechanism

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
//                .set("spark.sql.join.preferSortMergeJoin","false")                 //uncommnet to enable hash join
//                .set("spark.sql.autoBroadcastJoinThreshold","300000000")        //uncomment to increase limit of broadcast to 300 MB, thus performing broadcast hash join
                .set("spark.metrics.appStatusSource.enabled", "true");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .appName("MySparkApp")
                .master("spark://Rushi-Rajpara:7077")   //use local[*] if you have not set up any cluster
                .getOrCreate();

        System.out.println("AppID : " + spark.sparkContext().getConf().getAppId());

        Dataset<Row> dataset1 = spark.read().option("header",true).csv("/Users/rushirajpara/Documents/src/main/resources/finalTweets.csv");     //used files from local storage to avoid excess time of downloading them from s3
        Dataset<Row> dataset2 = spark.read().parquet("/Users/rushirajpara/Documents/src/main/resources/ddos4");

        dataset1 = dataset1.withColumn("extra",functions.rand().multiply(100).cast(DataTypes.IntegerType));
        dataset2 = dataset2.withColumn("extra",functions.rand().multiply(100).cast(DataTypes.IntegerType));
        dataset1.join(dataset2,"extra").show();

    }
}
