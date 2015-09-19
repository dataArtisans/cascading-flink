# Cascading Connector for Apache Flink

[Cascading](http://www.cascading.org/projects/cascading) is a popular framework to develop, maintain, and execute large-scale and robust data analysis applications. Originally, Cascading flows have been executed on [Apache Hadoop](http://hadoop.apache.org). The most recent [3.0 release](http://www.cascading.org/2015/06/08/cascading-3-0-release) of Cascading added support for [Apache Tez](http://tez.apache.org) as a runtime backend.

[Apache Flink](http://flink.apache.org) is a platform for scalable stream and batch processing. Flink's execution engine features low-latency pipelined and scalable batched data transfers and high-performance, in-memory operators for sorting and joining that gracefully go out-of-core in case of scarce memory resources. Flink can execute programs on a local machine, in a dedicated cluster, or on Hadoop YARN setups.

The **Cascading Connector for Apache Flink** enables you to execute Cascading flows on Apache Flink. Using this connector, your Cascading flows will benefit from Flink's runtime features such as its pipelined data shuffles and its efficient and robust in-memory operators.

## Features & Limitations ##

The Cascading Connector for Apache Flink supports most Cascading and Flink features. 

- Most Cascading operators are directly executed on Flink's memory-safe operators. This significantly reduces the need for cumbersome parameter tuning such as spill thresholds and the risk for `OutOfMemoryErrors`.
- Flink's runtime leverages field type information of Cascading programs. Apache Flink uses specialized serializers and comparators to efficiently operate on binary data. Cascading flows that specify the type of key fields benefit from significant performance improvements.

However, there are also a few limitations, which we are still working on, namely:

- Only InnerJoins for HashJoin pipes. The remaining join types will be available once Flink supports hash-based outer joins.

## Install ##

**WARNING** The Cascading Connector for Apache Flink does currently depend on unstable 0.10-SNAPSHOT builds of Apache Flink. 
Since Flink APIs may be broken at any time, building the connector might not succeed. Please drop a note to fabian [at] data-artisans.com in this case.
We will fix the Flink dependency as soon as Apache Flink releases a 0.10 version. 

To retrieve the latest version of the Cascading Connector for Apache Flink, run the following command

    git clone https://github.com/dataArtisans/cascading-flink.git

Then switch to the newly created directory and run Maven to build the Cascading Connector for Apache Flink:

    cd cascading-flink
    mvn clean install -DskipTests

The Cascading Connector for Apache Flink is now installed in your local maven repository.

## WordCount Example

Next, let's run the classic WordCount example. Before running it in our ETL pipeline on the cluster,
we want to run it locally first. The Cascading Connector for Apache Flink contains a built-in local execution mode to test
your applications before deploying them.

Here is the main method of our WordCount example:

```java

public static void main(String[] args) {

    Fields token = new Fields( "token" );
    Fields text = new Fields( "text" );
    RegexSplitGenerator splitter = new RegexSplitGenerator( token, "\\s+" );
    // only returns "token"
    Pipe docPipe = new Each( "token", text, splitter, Fields.RESULTS );

    Pipe wcPipe = new Pipe( "wc", docPipe );
    wcPipe = new AggregateBy( wcPipe, token, new CountBy(new Fields("count")));

    Tap inTap = new Hfs(new TextDelimited(text, "\n" ), args[0]);
    Tap outTap = new Hfs(new TextDelimited(false, "\n"), args[1]);

    FlowDef flowDef = FlowDef.flowDef().setName( "wc" )
            .addSource( docPipe, inTap )
            .addTailSink( wcPipe, outTap );

    FlowConnector flowConnector = new FlinkConnector();

	Flow wcFlow = flowConnector.connect( flowDef );

    wcFlow.complete();
}
```

That's really all the code you need to run a WordCount.

## Local execution

Now let's see how our WordCount performs.

To execute the example, let's first get some sample data:

    curl http://www.gutenberg.org/cache/epub/1128/pg1128.txt > kinglear.txt

Then let's run the included WordCount locally on your machine:

    mvn exec:exec -Dinput=kinglear.txt -Doutput=wordcounts.txt

Congratulations! You have run your first Cascading application on top of Apache Flink.

## Cluster Execution

To execute your program on a cluster, you need to build a "fat jar" containing your program and the
Flink-Cascading dependencies. Use the [assembly plugin](http://maven.apache.org/plugins/maven-assembly-plugin/)
or the [shading plugin](https://maven.apache.org/plugins/maven-shade-plugin/) to achieve that.

Then submit the jar to a Flink cluster by using the command-line utility:

    cd flink-dist
    cd bin
    ./flink run -c <entry_class> <path_to_jar> <jar_arguments>
 
 This will use the cluster's default parallelism. You may explicitly specify the parallelism by 
 setting the `-p` flag.
 
    ./flink run -p 20 -c <entry_class> <path_to_jar> <jar_arguments>
    
This will set the default parallelism for operators to 20 which means Flink tries to run 20 parallel
instances of each operation.

### YARN cluster

If you want to run your Flink cluster on YARN, you can do that as follows:

    ./flink run -m yarn-cluster -yn 10 -p 20 -c <entry_class> <path_to_jar> <jar_arguments>

This will create a Flink cluster for your job consisting of 10 task managers (workers) with a default 
parallelism of 20.

### WordCount on the Cluster

Now let's run the included WordCount example on the cluster.

    ./flink run -c com.dataartisans.flinkCascading.example.WordCount cascading-flink.jar hdfs:///input hdfs:///output

Or on a YARN cluster:

    ./flink run  -m yarn-cluster -yn 10 -c com.dataartisans.flinkCascading.example.WordCount cascading-flink.jar hdfs:///input hdfs:///output
