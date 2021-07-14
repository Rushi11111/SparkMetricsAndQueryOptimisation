import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Scanner;

class Restaurant implements Serializable {
    String url;
    String address;
    String name;
    String online_order;
    String book_table;
    String rate;
    Integer votes;
    String phone;
    String location;
    String rest_type;
    String dish_liked;
    String cuisines;
    Integer approx_cost;
    String reviews_list;
    String menu_item;
    String listed_in_type;
    String listed_in_city;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOnline_order() {
        return online_order;
    }

    public void setOnline_order(String online_order) {
        this.online_order = online_order;
    }

    public String getBook_table() {
        return book_table;
    }

    public void setBook_table(String book_table) {
        this.book_table = book_table;
    }

    public String getRate() {
        return rate;
    }

    public void setRate(String rate) {
        this.rate = rate;
    }

    public Integer getVotes() {
        return votes;
    }

    public void setVotes(Integer votes) {
        this.votes = votes;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getRest_type() {
        return rest_type;
    }

    public void setRest_type(String rest_type) {
        this.rest_type = rest_type;
    }

    public String getDish_liked() {
        return dish_liked;
    }

    public void setDish_liked(String dish_liked) {
        this.dish_liked = dish_liked;
    }

    public String getCuisines() {
        return cuisines;
    }

    public void setCuisines(String cuisines) {
        this.cuisines = cuisines;
    }

    public Integer getApprox_cost() {
        return approx_cost;
    }

    public void setApprox_cost(Integer approx_cost) {
        this.approx_cost = approx_cost;
    }

    public String getReviews_list() {
        return reviews_list;
    }

    public void setReviews_list(String reviews_list) {
        this.reviews_list = reviews_list;
    }

    public String getMenu_item() {
        return menu_item;
    }

    public void setMenu_item(String menu_item) {
        this.menu_item = menu_item;
    }

    public String getListed_in_type() {
        return listed_in_type;
    }

    public void setListed_in_type(String listed_in_type) {
        this.listed_in_type = listed_in_type;
    }

    public String getListed_in_city() {
        return listed_in_city;
    }

    public void setListed_in_city(String listed_in_city) {
        this.listed_in_city = listed_in_city;
    }
}

public class adhocQueries {

    public static void main(String[] args) {
//        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .set("spark.metrics.conf.*.sink.graphite.class", "org.apache.spark.metrics.sink.GraphiteSink")
                .set("spark.metrics.conf.*.sink.graphite.host", "127.0.0.1")
                .set("spark.metrics.conf.*.sink.graphite.port", "2003")
                .set("spark.metrics.conf.*.sink.graphite.period", "1")
                .set("spark.metrics.conf.*.sink.graphite.unit", "seconds")
                .set("spark.metrics.conf.*.sink.graphite.prefix", "tweets-app")
                .set("fs.s3a.endpoint", xyz)
                .set("fs.s3a.access.key", abc)
                .set("fs.s3a.secret.key", dfg)
                .set("spark.metrics.conf.*.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
                .set("spark.metrics.appStatusSource.enabled", "true");
//                .set("spark.default.parallelism","1")
//                .set("spark.sql.autoBroadcastJoinThreshold","300000000")
//                .set("spark.sql.parquet.columnarReaderBatchSize","128");


        SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .appName("CustomSparkApp")
//                .master("local[1]")
                .master("spark://Rushi-Rajpara:7077")
                .getOrCreate();

        System.out.println("AppID : " + spark.sparkContext().getConf().getAppId());

//        =============================================Making Parquet======================================================

//        StructType type = new StructType()
//                .add("url", DataTypes.StringType,true)
//                .add("address", DataTypes.StringType,true)
//                .add("name", DataTypes.StringType,true)
//                .add("online_order", DataTypes.StringType,true)
//                .add("book_table", DataTypes.StringType,true)
//                .add("rate", DataTypes.StringType,true)
//                .add("votes", DataTypes.IntegerType,true)
//                .add("phone", DataTypes.StringType,true)
//                .add("location", DataTypes.StringType,true)
//                .add("rest_type", DataTypes.StringType,true)
//                .add("dish_liked", DataTypes.StringType,true)
//                .add("cuisines", DataTypes.StringType,true)
//                .add("approx_cost", DataTypes.IntegerType,true)
//                .add("reviews_list", DataTypes.StringType,true)
//                .add("menu_item", DataTypes.StringType,true)
//                .add("listed_in_type", DataTypes.StringType,true)
//                .add("listed_in_city", DataTypes.StringType,true);

//        Dataset<Row> dataset = spark.read().option("header",true).option("escape","\"").option("quote", "\"").option("multiLine", "true").schema(type).csv("src/main/resources/zomato.csv");
//        dataset.write().parquet("s3a://learningsparkrr/parqZomato");

//        =============================================Create Dataset======================================================
//        Dataset<Row> dataset = spark.read().parquet("/Users/rushirajpara/Documents/src/main/resources/file123");      //Dataframe
//        Dataset<Row> dataset1 = spark.read().parquet("/Users/rushirajpara/Documents/src/main/resources/parqTweetsSP");
//        Dataset<Row> dataset2 = spark.read().parquet("/Users/rushirajpara/Documents/src/main/resources/ddos4");
//        Dataset<Row> dataset3 = spark.read().option("header",true).csv("/Users/rushirajpara/Documents/src/main/resources/finalTweets.csv");

        Dataset<Row> dataset = spark.read().parquet("s3a://learningsparkrr/parqZomato");      //Dataframe through s3
//        Dataset<Row> dataset1 = spark.read().parquet("s3a://learningsparkrr/parqTweets");
//        Dataset<Row> dataset2 = spark.read().parquet("s3a://learningsparkrr/ddos1");

//        Dataset<Restaurant> dataset = spark.read().parquet("/Users/rushirajpara/Desktop/parqZomato").as(Encoders.bean(Restaurant.class));

//        System.out.println("Number of Partitions : " + dataset.toJavaRDD().getNumPartitions());
//        ==============================================Queries============================================================
//        dataset.sort(functions.desc("votes")).show(10);

//        dataset.limit(1);

//        dataset.select(functions.col("name"), functions.col("phone")).sort(functions.desc("votes")).show();

//        dataset.sort(functions.desc("votes")).select(functions.col("name"), functions.col("phone"));


//        dataset = dataset.where(functions.col("approx_cost").$less$eq(500)).sort(functions.desc("approx_cost"));
//        dataset.show();


//        dataset.persist();
//        Dataset<Row> dataset1 = dataset.select("name","votes").where(functions.col("approx_cost").$less$eq(500)).sort(functions.desc("approx_cost"));
//        Dataset<Row> dataset2 = dataset.select("name","approx_cost");
//        dataset1 = dataset1.join(dataset2,"name");
//        dataset1.explain();
//        dataset1.show();

//        dataset.createOrReplaceTempView("table1");
//        spark.sql("SELECT name FROM table1 WHERE listed_in_city IN ('Banashankari')").show();

//        dataset.select("name").where(functions.col("listed_in_city").isin("Banashankari")).show();
//
//        dataset.createOrReplaceTempView("table1");
//        spark.sql("SELECT * FROM table1 WHERE reviews_list like '%bad%'").show();

//        dataset.createOrReplaceTempView("table1");
//        spark.sql("SELECT * FROM table1 WHERE online_order = 'Yes' OR online_order = 'No'").show();

//        dataset.createOrReplaceTempView("table1");
//        spark.sql("SELECT * FROM table1 LIMIT 1").show();

//        dataset.select(functions.col("name")).where(functions.col("reviews_list").like("%bad%")).show();

//        dataset.createOrReplaceTempView("zomato");
//        spark.sql("SELECT name,phone,listed_in_city FROM zomato WHERE ");

//        dataset1.createOrReplaceTempView("tweets");
//        spark.sql("SELECT user_name,user_screen_name,state FROM tweets ORDER BY likes DESC").show();

//        System.out.println(dataset1.toJavaRDD().take(1));

//        dataset1.createOrReplaceTempView("tweets");
//        spark.sql("SELECT * FROM tweets WHERE likes >= 160000").show(1);


//        System.out.println(dataset.first());

//        List<Row> rws= dataset.takeAsList(1);             //FETCH ONE PARTITION
//        System.out.println(rws.get(0));

//        dataset.createOrReplaceTempView("table");
//        spark.sql("SELECT * FROM table WHERE reviews_list LIKE '%Rated 5.0%'").show();

//        dataset = dataset.coalesce(4);
//        dataset.groupBy("name").agg(functions.collect_set(functions.col("reviews_list"))).show();

//        dataset2.createOrReplaceTempView("dataset2");
//        spark.sql("" +
//                "SELECT * " +
//                "FROM dataset2 " +
//                "WHERE label='ddos' AND InitBwdWinByts >= 800 AND BwdSegSizeAvg >= 120 AND PktSizeAvg>=650 " +
//                "AND Protocol = 6 AND FwdHeaderLen >= 400").show();

//        spark.udf().register("udf1",(Integer x) -> {
//            if(x%2 == 1) {
//                return true;
//            } else {
//                return false;
//            }
//        }, DataTypes.BooleanType);
//
//        dataset2.withColumn("extra",functions.callUDF("udf1",functions.col("FwdHeaderLen"))).select("label","FwdHeaderLen","extra").show();

//        dataset3 = dataset3.withColumn("extra",functions.rand().multiply(100).cast(DataTypes.IntegerType));
//        dataset2 = dataset2.withColumn("extra",functions.rand().multiply(100).cast(DataTypes.IntegerType));
//        dataset3.join(dataset2,"extra").show();


//        dataset2.groupBy("FwdHeaderLen").count().withColumnRenamed("count","newGrp").groupBy("newGrp").agg(functions.sum("FwdHeaderLen")).show();

//        dataset.sample(1/50000.0).show();

//        dataset.repartition()

//        System.out.println(dataset.count());

//        dataset2.repartition(1000).write().parquet("/Users/rushirajpara/Documents/src/main/resources/ddos2");

//        dataset2.where(functions.col("SrcPort").$greater(100000)).show();
//        dataset2.show();

//        dataset.where(functions.col("votes").$greater(1000000)).show();

//        System.out.println(dataset.count());
//        dataset.where("votes > 16500").show();           //PREDICATE PUSH DOWN
//        dataset2.where(functions.col("FlowIATMean").$greater(50000000).and(functions.col("FlowIATStd").$greater(10000000))).show();
//        dataset2.show();

//        dataset.limit(10).show();

//        dataset.withColumn("PartitionID",functions.spark_partition_id()).groupBy("PartitionID").count().show();
//        dataset2.where("FlowIATMean >= 100000000000000000000000").show();

//        dataset.where("votes > 14000").show();

//        dataset2.sort("TotBwdPkts").show();

//        dataset2.limit(100).groupBy("TotBwdPkts").sum().show();

//        List<Row> data = dataset.takeAsList(11000);
//        for(Row rw : data)
//        {
//            System.out.println(rw);
//        }


//        dataset.createOrReplaceTempView("table");
//        spark.sql("SELECT * FROM table").show(1);

//        dataset.where("votes >= 14000").show(10);
//        dataset2.withColumn("Partition",functions.spark_partition_id()).select("Partition").groupBy("Partition").count().show();

//        dataset.limit(4000).show();
//        dataset.limit(1).show();
//        dataset.limit(10).select("votes").groupBy().sum("votes").show();

//        dataset2.createOrReplaceTempView("table");
//        spark.sql("SELECT * FROM table LIMIT 10000000").show();


//        dataset.show(100000);

        System.out.println("completed");
        Scanner scan = new Scanner(System.in);
        scan.nextLine();

    }
}
