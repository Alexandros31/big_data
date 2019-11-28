package project;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class Comparisons {

    private static final String BREXIT = "brexit";
    private static final String LOVE = "love";
    private static final String TRAVEL = "travel";
    private static final String TRUMP = "trump";
    private static final String REPOST = "repost";

    private static final List<String> topics = new ArrayList<>(Arrays.asList(BREXIT, LOVE, TRAVEL, TRUMP, REPOST));

    // Utility functions

    private static int getRelevantTweetsBatch(final String topic) {
        int count = 0;
        for (int i = 3; i < 5; i++) {
            count += relevantTweets(topic, "/Users/alex/Desktop/Stuff/Big Data/project/project/tweetsBatch/" + i);
        }
        return count;
    }


    private static int getLikesBatch(final String topic) {
        int count = 0;
        for (int i = 3; i < 5; i++) {
            count += likes(topic, "/Users/alex/Desktop/Stuff/Big Data/project/project/likesBatch/" + i);
        }
        return count;
    }


    private static int getRetweetsBatch(final String topic) {
        int count = retweets(topic, "/Users/alex/Desktop/Stuff/Big Data/project/project/retweetsBatch/1");
        for (int i = 3; i < 5; i++) {
            count += retweets(topic, "/Users/alex/Desktop/Stuff/Big Data/project/project/retweetsBatch/" + i);
        }
        return count;
    }


    private static int getMentionsBatch(final String topic) {
        int count = 0;
        for (int i = 3; i < 5; i++) {
            count += mentions(topic, "/Users/alex/Desktop/Stuff/Big Data/project/project/mentionsBatch/" + i);
        }
        return count;
    }


    private static List<String> getLocationsBatch(final String topic) {
        List<String> list = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            list.addAll(locations(topic, "/Users/alex/Desktop/Stuff/Big Data/project/project/locationsBatch/" + i));
        }
        return list;
    }


    private static int relevantTweets(final String topic, final String path) {

        int count = 0;

        try {
            Scanner allTweets = new Scanner(new File(path));
            while (allTweets.hasNextLine()) {
                String line = allTweets.nextLine();
                int len = line.length();
                if (line.substring(1, 5).equals(topic)) {
                    count += Integer.parseInt(line.substring(6, len-1));
                }
                else if (line.substring(1, 6).equals(topic)) {
                    count += Integer.parseInt(line.substring(7, len-1));
                }
                else if (line.substring(1, 7). equals(topic)) {
                    count += Integer.parseInt(line.substring(8, len-1));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return count;
    }


    private static int likes(final String topic, final String path) {

        int count = 0;

        try {
            Scanner allTweets = new Scanner(new File(path));
            while (allTweets.hasNextLine()) {
                String line = allTweets.nextLine();
                int len = line.length();
                if (line.substring(1, 5).equals(topic)) {
                    count += Integer.parseInt(line.substring(6, len-1));
                }
                else if (line.substring(1, 6).equals(topic)) {
                    count += Integer.parseInt(line.substring(7, len-1));
                }
                else if (line.substring(1, 7). equals(topic)) {
                    count += Integer.parseInt(line.substring(8, len-1));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return count;
    }


    private static int retweets(final String topic, final String path) {

        int count = 0;

        try {
            Scanner allTweets = new Scanner(new File(path));
            while (allTweets.hasNextLine()) {
                String line = allTweets.nextLine();
                int len = line.length();
                if (line.substring(4, 8).equals(topic)) {
                    count += Integer.parseInt(line.substring(9, len - 1));
                } else if (line.substring(4, 9).equals(topic)) {
                    count += Integer.parseInt(line.substring(10, len - 1));
                } else if (line.substring(4, 10).equals(topic)) {
                    count += Integer.parseInt(line.substring(11, len - 1));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return count;
    }


    private static int mentions(final String topic, final String path) {

        int count = 0;

        try {
            Scanner allTweets = new Scanner(new File(path));
            while (allTweets.hasNextLine()) {
                String line = allTweets.nextLine();
                int len = line.length();
                if (line.substring(3, 7).equals(topic)) {
                    count += Integer.parseInt(line.substring(8, len - 1));
                } else if (line.substring(3, 8).equals(topic)) {
                    count += Integer.parseInt(line.substring(9, len - 1));
                } else if (line.substring(3, 9).equals(topic)) {
                    count += Integer.parseInt(line.substring(10, len - 1));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return count;
    }


    private static List<String> locations(final String topic, final String path) {

        List<String> list = new ArrayList<>();

        try {
            Scanner allTweets = new Scanner(new File(path));
            while (allTweets.hasNextLine()) {
                String line = allTweets.nextLine();
                int len = line.length();
                if (line.substring(len-5, len-1).equals(topic)) {
                    list.add(line.substring(1, len-6));
                } else if (line.substring(len-6, len-1).equals(topic)) {
                    list.add(line.substring(1, len-7));
                } else if (line.substring(len-7, len-1).equals(topic)) {
                    list.add(line.substring(1, len-8));
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return list;
    }



    // Comparison and prediction metrics


    public class TweetDensity extends RichSinkFunction<Tuple2<String, Integer>> {

        private Map<String, Integer> tweets;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.tweets = new HashMap<>();
            for (String topic : topics) {
                Integer count = getRelevantTweetsBatch(topic);
                tweets.put(topic, count);
            }
        }


        @Override
        public void invoke(Tuple2<String, Integer> relevantTweets, Context context) throws Exception {

            final String topic = relevantTweets.f0;
            final double batchDensity = this.tweets.get(topic) / 5;
            final double streamDensity = relevantTweets.f1 / 0.25;
            final double prediction = (batchDensity + streamDensity) / 2;

            // compare
            System.out.println("\nThe average amount of relevant tweets for topic \"" + topic + "\" relative to time (minutes) are:");
            System.out.println("Batch value: " + batchDensity + "\nStream value: " + streamDensity);
            // predict
            System.out.println("The predicted density for relevant tweets for topic \""+ topic + "\" per minute is: " + prediction);
        }

    }

    public class LikeRatio extends RichSinkFunction<Tuple2<String, Integer>> {

        private Map<String, Integer> likes;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.likes = new HashMap<>();
            for (String topic : topics) {
                Integer count = getLikesBatch(topic);
                this.likes.put(topic, count);
            }
        }


        @Override
        public void invoke(Tuple2<String, Integer> likedTweets, Context context) throws Exception {
            final String topic = likedTweets.f0;
            final double batchDensity = this.likes.get(topic) / 5;
            final double streamDensity = likedTweets.f1 / 0.25;
            final double prediction = (batchDensity + streamDensity) / 2;

            // compare
            System.out.println("\nThe average amount of likes for tweets with topic \"" + topic + "\" relative to time (minutes) are:");
            System.out.println("Batch value: " + batchDensity + "\nStream value: " + streamDensity);
            // predict
            System.out.println("The predicted ratios for liked tweets for topic \""+ topic + "\" per minute is: " + prediction);
        }

    }

    public class RetweetRatio extends RichSinkFunction<Tuple2<String, Integer>> {

        private Map<String, Integer> retweets;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.retweets = new HashMap<>();
            for (String topic : topics) {
                Integer count = getRetweetsBatch(topic);
                this.retweets.put("RT "+topic, count);
            }
        }


        @Override
        public void invoke(Tuple2<String, Integer> retweetedTweets, Context context) throws Exception {
            final String topic = retweetedTweets.f0;
            if (!this.retweets.containsKey(topic)) {
                return;
            }
            final double batchDensity = this.retweets.get(topic) / 5;
            final double streamDensity = retweetedTweets.f1 / 0.25;
            final double prediction = (batchDensity + streamDensity) / 2;

            // compare
            System.out.println("\nThe average amount of retweets for tweets with topic \"" + topic + "\" relative to time (minutes) are:");
            System.out.println("Batch value: " + batchDensity + "\nStream value: " + streamDensity);
            // predict
            System.out.println("The predicted ratios for retweeted tweets for topic \""+ topic + "\" per minute is: " + prediction);
        }

    }

    public class MentionsComparison extends RichSinkFunction<Tuple2<String, Integer>> {

        private Map<String, Integer> mentions;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.mentions = new HashMap<>();
            for (String topic : topics) {
                Integer count = getMentionsBatch(topic);
                this.mentions.put("@ "+topic, count);
            }
        }


        @Override
        public void invoke(Tuple2<String, Integer> mentionedTweets, Context context) throws Exception {
            final String topic = mentionedTweets.f0;
            if (!this.mentions.containsKey(topic)) {
                return;
            }
            final double batchDensity = this.mentions.get(topic) / 5;
            final double streamDensity = mentionedTweets.f1 / 0.25;

            // compare
            System.out.println("\nThe average amount of retweets for tweets with topic \"" + topic + "\" relative to time (minutes) are:");
            System.out.println("Batch value: " + batchDensity + "\nStream value: " + streamDensity);
        }

    }


    public class LocationsComparison extends RichSinkFunction<Tuple2<String, String>> {

        private Map<String, List<String>> locations;

        @Override
        public void open(Configuration parameters) throws Exception {
            this.locations = new HashMap<>();
            for (String topic : topics) {
                List<String> list = getLocationsBatch(topic);
                this.locations.put(topic, list);
            }
        }


        @Override
        public void invoke(Tuple2<String, String> mentionedTweets, Context context) throws Exception {
            final String location = mentionedTweets.f0;
            final String topic = mentionedTweets.f1;
            if (!this.locations.containsKey(topic)) {
                return;
            }

            final boolean exists = this.locations.get(topic).contains(location);

            // compare
            if (exists) {
                System.out.println("There have been tweets from location: \"" + location + "\" in both stream and batch data for topic: \"" + topic + "\".");
            }
            else {
                System.out.println("There have not been tweets from location: \"" + location + "\" in the batch set for topic: \"" + topic + "\".");
            }
        }

    }

}
