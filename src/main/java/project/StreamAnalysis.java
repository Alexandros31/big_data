/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package project;

import javassist.expr.Instanceof;
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
public class StreamAnalysis {

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

	private static Integer allTweets = 0;


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

		DataStream<Tuple2<String, Integer>> allTweets = source
				// selecting tweets by topic and splitting to (topic, 1)
				.flatMap(new allTweets())
				// group by tweets
				.keyBy(0)
				.timeWindow(Time.seconds(15))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
						return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
					}
				});

		DataStream<Tuple2<String, Integer>> tweets = source
				// selecting tweets by topic and splitting to (topic, 1)
				.flatMap(new Count())
				// group by tweets
				.keyBy(0)
				.timeWindow(Time.seconds(15))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
						return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
					}
				});

		DataStream<Tuple2<String, Integer>> retweets = source
				// selecting tweets by topic and splitting to (topic, 1)
				.flatMap(new Retweet())
				// group by tweets
				.keyBy(0)
				.timeWindow(Time.seconds(15))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
						return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
					}
				});

		DataStream<Tuple2<String, Integer>> mentions = source
				// selecting tweets by topic and splitting to (topic, 1)
				.flatMap(new Mention())
				// group by tweets
				.keyBy(0)
				.timeWindow(Time.seconds(15))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
						return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
					}
				});


		DataStream<Tuple2<String, String>> locations = source
				// selecting tweets by topic and splitting to (topic, 1)
				.flatMap(new Location())
				// group by tweets
				.keyBy(0)
				.timeWindow(Time.seconds(15))
				.reduce(new ReduceFunction<Tuple2<String, String>>() {
					@Override
					public Tuple2<String, String> reduce(Tuple2<String, String> stringStringTuple2, Tuple2<String, String> t1) throws Exception {
						return new Tuple2<>(stringStringTuple2.f0, t1.f1);
					}
				});

		DataStream<Tuple2<String, Integer>> likes = source
				// selecting tweets by topic and splitting to (topic, 1)
				.flatMap(new Likes())
				// group by tweets
				.keyBy(0)
				.timeWindow(Time.seconds(15))
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
						return new Tuple2<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1);
					}
				});


		// allTweets.print();
		/*
		tweets.print();
		retweets.print();
		mentions.print();
		locations.print();
		likes.print();

		allTweets.writeAsText("/Users/alex/Desktop/Stuff/Big Data/project/project/allTweets");
		tweets.writeAsText("/Users/alex/Desktop/Stuff/Big Data/project/project/tweets");
		retweets.writeAsText("/Users/alex/Desktop/Stuff/Big Data/project/project/retweets");
		mentions.writeAsText("/Users/alex/Desktop/Stuff/Big Data/project/project/mentions");
		locations.writeAsText("/Users/alex/Desktop/Stuff/Big Data/project/project/locations");
		likes.writeAsText("/Users/alex/Desktop/Stuff/Big Data/project/project/likes");

		*/
		Comparisons comparisons = new Comparisons();

		Comparisons.TweetDensity tweetDensity = comparisons.new TweetDensity();
		// tweets.addSink(tweetDensity).setParallelism(1);

		Comparisons.LikeRatio likeRatio = comparisons.new LikeRatio();
		// likes.addSink(likeRatio).setParallelism(1);

		Comparisons.RetweetRatio retweetRatio = comparisons.new RetweetRatio();
		// retweets.addSink(retweetRatio).setParallelism(1);

		Comparisons.MentionsComparison mentionsComparison = comparisons.new MentionsComparison();
		// mentions.addSink(mentionsComparison).setParallelism(1);

		Comparisons.LocationsComparison locationsComparison = comparisons.new LocationsComparison();
		// locations.addSink(locationsComparison).setParallelism(1);

		env.execute("Stream Analysis");

	}

	private static String getTopic(String tweet) {
		String topic = "";
		StringTokenizer tokenizer = new StringTokenizer(tweet);
		while (tokenizer.hasMoreTokens()) {
			String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();
			if (topics.contains(result)) {
				topic = result;
			}
		}
		return topic;
	}


	private static class allTweets implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			out.collect(new Tuple2<>("", 1));
		}
	}


	private static class Count implements FlatMapFunction<String, Tuple2<String, Integer>> {

		private transient ObjectMapper jsonParser;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean hasText = jsonNode.has("text");
			if (hasText) {
				String topic = getTopic(jsonNode.get("text").asText());
				if (!topic.isEmpty()) {
					out.collect(new Tuple2<>(topic, 1));
				}
			}
		}
	}


	private static class Retweet implements FlatMapFunction<String, Tuple2<String, Integer>> {

		private transient ObjectMapper jsonParser;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean hasText = jsonNode.has("text");
			if (hasText) {
				String tweet = jsonNode.get("text").asText();
				if (tweet.length() > 2) {
					if (tweet.substring(0, 2).equals("RT")) {
						String topic = getTopic(tweet);
						if (!topic.isEmpty()) {
							out.collect(new Tuple2<>("RT " + topic, 1));
						}
					}
				}
			}
		}
	}

	private static class Mention implements FlatMapFunction<String, Tuple2<String, Integer>> {

		private transient ObjectMapper jsonParser;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean hasText = jsonNode.has("text");
			if (hasText) {
				String tweet = jsonNode.get("text").asText();
				if (tweet.substring(0, 1).equals("@")) {
					String topic = getTopic(tweet);
					if (!topic.isEmpty()) {
						out.collect(new Tuple2<>("@ " + topic, 1));
					}
				}
			}
		}
	}

	private static class Location implements FlatMapFunction<String, Tuple2<String, String>> {

		private transient ObjectMapper jsonParser;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {

			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean hasText = jsonNode.has("text");
			boolean hasUser = jsonNode.has("user");
			if (hasText && hasUser) {
				String location = jsonNode.get("user").get("location").asText();
				String topic = getTopic(jsonNode.get("text").asText());
				if (!topic.isEmpty() && !location.equals("null")) {
					out.collect(new Tuple2<>("[" + location + "]", topic));
				}
			}
		}
	}

	private static class Likes implements FlatMapFunction<String, Tuple2<String, Integer>> {

		private transient ObjectMapper jsonParser;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}
			JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
			boolean hasText = jsonNode.has("text");
			boolean hasUser = jsonNode.has("user");
			if (hasText && hasUser) {
				String likes = jsonNode.get("user").get("followers_count").asText();
				String topic = getTopic(jsonNode.get("text").asText());
				if (!topic.isEmpty()) {
					out.collect(new Tuple2<>(topic, Integer.parseInt(likes)));
				}
			}
		}
	}



}
