# Cascading on top of Apache Flink

Cascading-Flink enables you to use Apache Flink as an execution engine for your Cascading
applications.


## Install ##

To retrieve the latest version of Cascading-Flink, run the following command

    git clone https://github.com/dataArtisans/cascading-flink.git

Then switch to the newly created directory and run Maven to build Cascading-Flink:

    cd cascading-flink
    mvn clean install -DskipTests

Cascading-Flink is now installed in your local maven repository.

## WordCount Example

Next, let's run the classic WordCount example. Before running it in our ETL pipeline on the cluster,
we want to run it locally first. Cascading-Flink contains a built-in local execution mode to test
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

Congratulations! You have run your first Flink-Cascading application on top of Apache Flink.

## Cluster Execution

To execute your program on a cluster, you need to build a "fat jar" containing your program and the
Flink-Cascading dependencies. Use the [assembly plugin](http://maven.apache.org/plugins/maven-assembly-plugin/)
or the [shading plugin](https://maven.apache.org/plugins/maven-shade-plugin/) to achieve that.

Then submit the jar to a Flink cluster by using the command-line utility:

    cd flink-dist
    cd bin
    ./flink run -c <entry_class> <path_to_jar> <jar_arguments>
 
 This will use the cluster's default parallelism. You may explictly specify the parallelism by 
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

    ./flink run -c com.dataArtisans.flinkCascading.example.WordCount cascading-flink.jar hdfs:///input hdfs:///output

Or on a YARN cluster:

    ./flink run  -m yarn-cluster -yn 10 -c com.dataArtisans.flinkCascading.example.WordCount cascading-flink.jar hdfs:///input hdfs:///output


## More

For more information, please visit the [Apache Flink website](http://flink.apache.org) or send a
mail to the [mailinglists](http://flink.apache.org/community.html#mailing-lists).