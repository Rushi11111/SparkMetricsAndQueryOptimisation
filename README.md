# Spark Metrics And Query Optimisation
As part of my summer intern project at Sprinklr, I have created this repository. I have added my learnings on pushing various metrics provided by Spark to InfluxDB, adding custom metrics to Spark, creating Grafana dashboard to visualise metrics that are pushed to InfluxDB and optimising few queires using Grafana dashboard.

<h2>Adding Metrics Provided By Spark To InfluxDB</h2>

For pushing metrics that are provided by Spark to InfluxDB, I have used graphite sink of Spark (it sends metric to graphite, which is a time series database just like InfluxDB) and graphite source of InfluxDB. Basically, Spark will push data to our specified address and port number in graphite format and we will open a listner from InfluxDB to that address and port so that data gets imported to InfluxDB.

<b>NOTE</b> : Spark don't have a option of directly pushing metirc's data to InfluxDB.

Now to push data with proper tags and measurements in InfluxDB we will have to use templates and do some configuration in InfluxDB's config file. Learn more about templates here : <a>https://docs.influxdata.com/influxdb/v1.8/supported_protocols/graphite/#:~:text=tags%20and%20measurements.-,Templates,-Templates%20allow%20matching</a>.

Following is part that I have changed from default configs of InfluxDB:

```
###
### [[graphite]]
###
### Controls one or many listeners for Graphite data.
###

[[graphite]]
  # Determines whether the graphite endpoint is enabled.
  enabled = true
  #I have used graphite-test database for storing metric data from Spark.
  database = "graphite-test"
  retention-policy = ""
  bind-address = ":2003"
  protocol = "tcp"
  consistency-level = "one"

  # These next lines control how batching works. You should have this enabled
  # otherwise you could get dropped metrics or poor performance. Batching
  # will buffer points in memory if you have many coming in.

  # Flush if this many points get buffered
  batch-size = 5000

  # number of batches that may be pending in memory
  batch-pending = 10

  # Flush at least this often even if we haven't hit buffer limit
  batch-timeout = "1s"

  # UDP Read buffer size, 0 means OS default. UDP listener will fail if set above OS max.
  udp-read-buffer = 0

  ### This string joins multiple matching 'measurement' values providing more control over the final measurement name.
  separator = "."

  ### Default tags that will be added to all metrics.  These can be overridden at the template level
  ### or by tags extracted from metric
  # tags = ["region=us-east", "zone=1c"]

  ### Each template line requires a template pattern.  It can have an optional
  ### filter before the template and separated by spaces.  It can also have optional extra
  ### tags following the template.  Multiple tags should be separated by commas and no spaces
  ### similar to the line protocol format.  There can be only one default template.
  templates = [
    #JVM source
    "*.*.jvm.pools.* username.applicationid.process.namespace.namespace.measurement*",
    #YARN source
    "*.*.applicationMaster.* username.applicationid.namespace.measurement*",
    #shuffle service source
    "*.shuffleService.* username.namespace.measurement*",
    #streaming
    "*.*.*.spark.streaming.* username.applicationid.process.namespace.namespace.id.measurement*",
    #generic template for driver and executor sources
    "username.applicationid.process.namespace.measurement*"
    ]
```

That was about setting up InfluxDB to listen for data from localhost:2003. Now, we will have to setup Spark's config to send metrics to that port in graphite format. We can do it in 2 ways, first way is to edit metrics.properties file present in conf folder of spark. We have to add following lines to it.

```
*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=127.0.0.1
*.sink.graphite.port=2003
*.sink.graphite.period=1
*.sink.graphite.unit=seconds
*.sink.graphite.prefix=MySparkApp
master.source.jvm.class=org.apache.spark.metrics.source.JvmSource
worker.source.jvm.class=org.apache.spark.metrics.source.JvmSource
driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource
appStatusSource.enabled=true
```
Another way is to include this config in code of Spark App while starting Spark Session.
```
 SparkConf sparkConf = new SparkConf()
                .set("spark.metrics.conf.*.sink.graphite.class", "org.apache.spark.metrics.sink.GraphiteSink")
                .set("spark.metrics.conf.*.sink.graphite.host", "127.0.0.1")
                .set("spark.metrics.conf.*.sink.graphite.port", "2003")
                .set("spark.metrics.conf.*.sink.graphite.period", "1")
                .set("spark.metrics.conf.*.sink.graphite.unit", "seconds")
                .set("spark.metrics.conf.*.sink.graphite.prefix", "MySparkApp")
                .set("spark.metrics.conf.*.source.jvm.class", "org.apache.spark.metrics.source.JvmSource")
                .set("spark.metrics.appStatusSource.enabled", "true");
                
SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .appName("Any Name")
                .master("<Your Cluster Url> OR local[*]")
                .getOrCreate();
```

Now, you can submit a jar to spark-submit and you should be able to see metrics data in InfluxDB. Remeber to create database in InfluxDB before submitting jar (in my configs name of DB is graphite-test).

<h2>Creating Grafana Dashboard</h2>
<ul>
  <li>In Grafana, first of all we will have to add InfluxDB datasource. We can do so by going to Settings -> Datasources -> Add data source, then choose InfluxDB. Add address of InfluxDB in URL option (by default it would be localhost:8086, if you have some other address then change it to that address). Now, add name of database (I have used 'graphite-test'), user and password. Then save it.

Now, we have to create a new dashboard. After creating a dashboard, we will have to add 2 variables, we can do so by going to Setting -> Variables of dashboard. Configure varaibles as shown in below images.

![alt text](https://github.com/Rushi11111/Rushi11111/blob/main/variable_applicationid.png)
![alt text](https://github.com/Rushi11111/Rushi11111/blob/main/variable_username.png)

Now, we are ready to add panel to our dashboard and query InfluxDB to show data on that panel. We can add various type of panels like Guage, Stat, Graph (old), Timeseries etc. To learn about querying data with InfluxQL, refer <a>https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/</a>.
</li>

<li>That was way to create a custom dashboard from scratch, however you can use following json file to directly import dashboard without doing any of the above steps.
<a>(https://github.com/Rushi11111/Rushi11111/blob/main/dashboard.json)</a>. 
</li>

</ul>

<h2>Adding Custom Metrics To InfluxDB</h2>
Spark provides a lot of metrics (list of metrics: https://spark.apache.org/docs/3.1.2/monitoring.html#:~:text=List%20of%20available%20metrics%20providers), but for some use cases we may need some custom metrics.
<br><br>
Adding custom metrics is bit advanced and requires configuration of Spark source code and/or source code of other jars (like hadoop-aws and hadoop-hdfs-client for S3A metrics). In my case, I wanted to get metadata time, read time, seek time and some other metrics for reading/writing data from/to AWS S3.
<br><br>
In order to get custom metrics, we need to have knowledge of 2 things :
<ol>
  <li>Dropwizard Package For Metrics </li>
  Spark uses dropwizard metrics for getting all of the metrics that it provides. The core of dropwizard metrics is metric registery, which is used to register all of our metrics, it is collection of all metrics of an application. There are various classes that are provided in dropwizard metrics package for storing a metric like gauge, counters, histogram, etc. Each of this classes have their speciality and depending on use case we should choose a class for a metric. For more info, refer https://metrics.dropwizard.io/4.2.0/getting-started.html.
  <li>Spark Plugins</li>
  In Spark 3.0, new plugin framework was introduced. It allows user to run custom code on driver and executor side. There is a spark plugin interface which has 2 methods namely 'driverPlugin' and 'executorPlugin'. This methods expects to get 'DriverPlugin' class and 'ExecutorPlugin' class respectively. This class has a method called 'init', which can be used to register custom metrics for driver and every executor. For more, refer http://blog.madhukaraphatak.com/spark-plugin-part-1/ and http://blog.madhukaraphatak.com/spark-plugin-part-2/.
</ol>

<h3>Adding custom S3A metrics</h3>
In order to add custom S3A metrics, we need to customise hadoop code. Here is my whole code, https://github.com/Rushi11111/hadoop. I have used code from https://github.com/LucaCanali/hadoop authored by Luca Canali. Moreover, we have to create a plugin jar too, here is link of the code of plugin https://github.com/Rushi11111/SparkPlugin.
<br/><br/>
In the code of plugin, there is 'S3ATimeInstrumentation' class in package 'org.apache.hadoop.fs.s3a' where all variables that will hold metrics and functions to increments and get value of these variables (metrics) are declared. Now, in order to register this metric (as gauges) with metric registery there is another class with same name (i.e. 'S3ATimeInstrumentation') in package 'ch.cern.experimental'. Get functions can be used to return value to gauge.
<br/><br/>
In the code of hadoop, metrics are updated with help of increment functions defined in 'S3ATimeInstrumentation' class defined in package 'org.apache.hadoop.fs.s3a'. Here is a example of how 'CPU time during read' and 'Time during read' metrics are updated,
<br/><br/>

```
public synchronized int read() throws IOException {
    checkNotClosed();
    if (this.contentLength == 0 || (nextReadPos >= contentLength)) {
      return -1;
    }

    try {
      lazySeek(nextReadPos, 1);
    } catch (EOFException e) {
      return -1;
    }

    //Start measuring time-------------------------------------------------------------------------------------------------------------------------------------
    long startTime = System.nanoTime();
    long startCPUTime = 0L;
    if (IsCurrentThreadCPUTimeSupported) {
      startCPUTime = threadMXBean.getCurrentThreadCpuTime();
    }
    //---------------------------------------------------------------------------------------------------------------------------------------------------------

    // With S3Guard, the metadatastore gave us metadata for the file in
    // open(), so we use a slightly different retry policy.
    // read() may not be likely to fail, but reopen() does a GET which
    // certainly could.
    Invoker invoker = context.getReadInvoker();
    int byteRead = invoker.retry("read", pathStr, true,
        () -> {
          int b;
          try {
            b = wrappedStream.read();
          } catch (EOFException e) {
            return -1;
          } catch (SocketTimeoutException e) {
            onReadFailure(e, 1, true);
            b = wrappedStream.read();
          } catch (IOException e) {
            onReadFailure(e, 1, false);
            b = wrappedStream.read();
          }
          return b;
        });

    //Stop measuring time and update metric-----------------------------------------------------------------------------------------------------------------------
    long elapsedTimeMicrosec = (System.nanoTime() - startTime) / 1000L;
    S3ATimeInstrumentation.incrementTimeElapsedReadOps(elapsedTimeMicrosec);
    if (IsCurrentThreadCPUTimeSupported) {
      long deltaCPUTimeMicrosec = (threadMXBean.getCurrentThreadCpuTime() - startCPUTime) / 1000L;
      S3ATimeInstrumentation.incrementCPUTimeDuringRead(deltaCPUTimeMicrosec);
    }
    //------------------------------------------------------------------------------------------------------------------------------------------------------------

    

    if (byteRead >= 0) {
      pos++;
      nextReadPos++;
    }

    if (byteRead >= 0) {
      incrementBytesRead(1);
    }
    return byteRead;
  }
```
<h2>Queries Optimisation</h2>

<h3>Limit Query</h3>

Query <i>"SELECT * FROM table LIMIT 1"</i> or <i>dataset.limit(1).show()</i> consumed whole data from my parquet file. Here, size of my parquet folder is around 160 MB, has 8 row groups and 8 files.

Here, spark doesn't wait to read footer of all parquet files but try to estimate number of tasks to start. Following is how spark estimates number of tasks to start,

Step 1: Decide total bytes, totalBytes = (sum of size of all parquet files) + (number of parquet files) * (4 * 1024 * 1024) </br>
Step 2: Decide max bytes per core, bytesPerCore = totalBytes / (spark defualt parallelism) <br/>
Step 3: Decide max split bytes, maxSplitBytes = max(bytesPerCore, (spark max partition bytes) ) <br/>
Step 4: Now, spark will start ceil(totalBytes/maxSplitBytes) number of tasks. First task will be given first maxSplitBytes of file, second task will be given next maxSplitBytes of file, and so on. Row group will be assigned to task which has mid-point of that row group. For example in image shown below, row group 1 will be given to task 2, row group 2 will be given to task 4 and other tasks will just read footer.<br/>
<br/>
  <p align="center">
    <img src="https://github.com/Rushi11111/Rushi11111/blob/main/task_row_group.png" alt="drawing" width="500"/>
  </p>

In my case, I had 8 row groups (and files too) of size around 29 MB and default parallelism was 12. So, Spark was creating 12 tasks and 8 of them were given task of reading one row group each and other 4 tasks read only footer. In order to get one row, each of the 8 tasks had to read entire row group, thus Spark was basically reading every bit of data just to get me one row of dataset.

Here are stats :
![alt text](https://github.com/Rushi11111/Rushi11111/blob/main/limit_1_big.png)

In order to tune Spark to read less data, I decreased size of row groups and re-wrote my parquet file. In this case one task will be assigned multiple row groups and they will read only one row group for getting a row. Here are stats:
![alt text](https://github.com/Rushi11111/Rushi11111/blob/main/limit_1_small.png)

<h3>Equal To Query</h3>
Query <i>"SELECT * FROM table WHERE likes=10000"</i> or <i>dataset.where("likes=10000").show()</i> processed every bit of data from my dataset. Upon some analysis, I found that, there was no dictionary stored for chunks of 'likes' column. In order to make query faster and make it read less data, I had to increase dictionary size of parquet (by changing config 'parquet.dictionary.page.size'). Conversely, One could also decrease row group size (by changing config 'parquet.block.size') to make data of column chunks fit in default size of dictionary.

<br/>Stats for the query on parquet file without dictionary:
![alt text](https://github.com/Rushi11111/Rushi11111/blob/main/EqualToQueryWithoutDict.png)

Stats for the query on parquet file with dictionary:
![alt text](https://github.com/Rushi11111/Rushi11111/blob/main/EqualToQueryWithDict.png)
