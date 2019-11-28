package project;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

public class PostAnalysis {

    private static final String BREXIT = "brexit";
    private static final String LOVE = "love";
    private static final String TRAVEL = "travel";
    private static final String TRUMP = "trump";
    private static final String REPOST = "repost";

    private static final List<String> topics = new ArrayList<>(Arrays.asList(BREXIT, LOVE, TRAVEL, TRUMP, REPOST));

    private static int allStream;
    private static int allBatch;

    public static void main(String[] args) {

        allStream = getAllTweetCount();
        allBatch = getAllTweetCountBatch();

        Map<String, Float> tweetDensitiesStream = getTweetDensities();
        Map<String, Float> likeRatiosStream = getLikeRatios();
        Map<String, Float> rtRatiosStream = getRTRatios();

        Map<String, Float> tweetDensitiesBatch = getTweetDensitiesBatch();
        Map<String, Float> likeRatiosBatch = getLikeRatiosBatch();
        Map<String, Float> rtRatiosBatch = getRTRatiosBatch();

        prediction(tweetDensitiesStream, tweetDensitiesBatch, "densities");
        prediction(likeRatiosStream, likeRatiosBatch, "like per tweet ratios");
        prediction(rtRatiosStream, rtRatiosBatch, "retweet per tweet ratios");

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

    // STREAM

    private static Map<String, Float> getTweetDensities() {

        Map<String, Float> ratios = new HashMap<>();
        for (String topic : topics) {
            Integer tweets = getRelevantTweets(topic);
            Float ratio = (float) tweets / allStream;
            ratios.put(topic, ratio);
        }
        return ratios;
    }

    private static Map<String, Float> getLikeRatios() {

        Map<String, Float> ratios = new HashMap<>();
        for (String topic : topics) {
            Integer tweets = getRelevantTweets(topic);
            Integer likes = getLikes(topic);
            Float ratio = (float) likes / tweets;
            ratios.put(topic, ratio);
        }
        return ratios;
    }

    private static Map<String, Float> getRTRatios() {

        Map<String, Float> ratios = new HashMap<>();
        for (String topic : topics) {
            Integer tweets = getRelevantTweets(topic);
            Integer retweets = getRetweets(topic);
            Float ratio = (float) retweets / tweets;
            ratios.put(topic, ratio);
        }
        return ratios;
    }


    private static int getAllTweetCount() {

        int count = 0;

        try {
            Scanner allTweets = new Scanner(new File("/Users/alex/Desktop/Stuff/Big Data/project/project/allTweets"));
            while (allTweets.hasNextLine()) {
                String line = allTweets.nextLine();
                int len = line.length();
                count += Integer.parseInt(line.substring(2, len-1));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return count;
    }


    private static int getRelevantTweets(final String topic) {
        return relevantTweets(topic, "/Users/alex/Desktop/Stuff/Big Data/project/project/tweets");
    }


    private static int getLikes(final String topic) {
        return likes(topic, "/Users/alex/Desktop/Stuff/Big Data/project/project/likes");
    }


    private static int getRetweets(final String topic) {
       return retweets(topic, "/Users/alex/Desktop/Stuff/Big Data/project/project/retweets");
    }

    //BATCH

    private static Map<String, Float> getTweetDensitiesBatch() {

        Map<String, Float> ratios = new HashMap<>();
        for (String topic : topics) {
            Integer tweets = getRelevantTweetsBatch(topic);
            Float ratio = (float) tweets / allBatch;
            ratios.put(topic, ratio);
        }
        return ratios;
    }

    private static Map<String, Float> getLikeRatiosBatch() {

        Map<String, Float> ratios = new HashMap<>();
        for (String topic : topics) {
            Integer tweets = getRelevantTweetsBatch(topic);
            Integer likes = getLikesBatch(topic);
            Float ratio = (float) likes / tweets;
            ratios.put(topic, ratio);
        }
        return ratios;
    }

    private static Map<String, Float> getRTRatiosBatch() {

        Map<String, Float> ratios = new HashMap<>();
        for (String topic : topics) {
            Integer tweets = getRelevantTweetsBatch(topic);
            Integer retweets = getRetweetsBatch(topic);
            Float ratio = (float) retweets / tweets;
            ratios.put(topic, ratio);
        }
        return ratios;
    }

    private static int getAllTweetCountBatch() {

        int count = 0;

        try {
            Scanner allTweets = new Scanner(new File("/Users/alex/Desktop/Stuff/Big Data/project/project/allTweetsBatch/3"));
            while (allTweets.hasNextLine()) {
                String line = allTweets.nextLine();
                int len = line.length();
                count += Integer.parseInt(line.substring(1, len-1));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        return count;
    }

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

    private static void prediction(Map<String, Float> streamMap, Map<String, Float> batchMap, String prediction) {
        System.out.println("The " + prediction + " for tweet topics for the near future are:");
        for (String topic : topics) {
            System.out.println(topic + ": " + (streamMap.get(topic) + batchMap.get(topic)) / 2);
            //System.out.println(streamMap.get(topic)+ " " + batchMap.get(topic));
        }
        System.out.println();
    }
}
