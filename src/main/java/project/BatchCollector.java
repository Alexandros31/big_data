
package project;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class BatchCollector {

    private static final String CONSUMER_KEY = "ceVKCDNpvbBfWRR14jSXWNFpU";
    private static final String CONSUMER_SECRET = "bhJq0xLnjIq1jt0aq6FoUMn6qMe7iKS3HqhxA4EB5hduYWtA99";
    private static final String ACCESS_TOKEN = "3080483959-5YGLQx5YxkZzObUullRG4qQXFbqQBRpzVks4lAs";
    private static final String ACCESS_TOKEN_SECRET = "R0eDiHIIbsVAVjfg6rDp8QcM3DEBFy4X6jQVmHxBdiher";

    private static final String BREXIT = "brexit";
    private static final String LOVE = "love";
    private static final String TRAVEL = "travel";
    private static final String TRUMP = "trump";
    private static final String REPOST = "repost";

    private static final List<String> topics = new ArrayList<>(Arrays.asList(BREXIT, LOVE, TRAVEL, TRUMP, REPOST));


    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty(TwitterSource.CONSUMER_KEY, CONSUMER_KEY);
        properties.setProperty(TwitterSource.CONSUMER_SECRET, CONSUMER_SECRET);
        properties.setProperty(TwitterSource.TOKEN, ACCESS_TOKEN);
        properties.setProperty(TwitterSource.TOKEN_SECRET, ACCESS_TOKEN_SECRET);

        DataStream<String> source = env.addSource(new TwitterSource(properties));


        source.writeAsText("/Users/alex/Desktop/Stuff/Big Data/project/project/TweetBatch");

        env.execute("Count tweets with certain topics");

    }

}

