package net.davidmcginnis.kafka.twittersentiment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Hello world!
 *
 */
public class App 
{
    private static Properties _props = new Properties();
    private static StanfordCoreNLP _pipeline;

    public static void main( String[] args )
    {
        _props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
        _pipeline = new StanfordCoreNLP(_props);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        ObjectMapper mapper = new ObjectMapper();
        KStream<String, String> twitterStream = builder.stream("twitter");
        KStream<String, Long> sentimentStream = twitterStream.map((String key, String value) -> {
            try {
                JsonNode nodes = mapper.readValue(value, JsonNode.class);

                return extractSentiment(nodes.get("payload").get("text").textValue());
            } catch(IOException e) {
                return new KeyValue<>("<NULL>", -1L);
            }
        });
        KStream<String, Long> filteredStream = sentimentStream.filter((String key, Long value) -> value >= 0);
        filteredStream.map((String key, Long value) -> new KeyValue<>(key, value.toString())).to("twitter-sentiment");

        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    private static KeyValue<String, Long> extractSentiment(String text) {
        List<KeyValue<String, Integer>> sentiments = extractSentiments(text);
        if (sentiments.isEmpty()) {
            return new KeyValue<>("<NO TWEET>", -1L);
        }
        else {
            return new KeyValue<>(text, getMean(sentiments).longValue());
        }
    }

    private static Double getMean(List<KeyValue<String, Integer>> sentiments) {
        double sum = 0.0;
        for (KeyValue<String, Integer> kvp : sentiments) {
            sum += kvp.value;
        }
        return sum / (double)sentiments.size();
    }

    private static List<KeyValue<String, Integer>> extractSentiments(String text) {
        Annotation annotation = _pipeline.process(text);
        List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
        List<KeyValue<String, Integer>> results = new ArrayList<>();
        for(CoreMap sentence : sentences) {
            Tree annotations = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
            Integer prediction = RNNCoreAnnotations.getPredictedClass(annotations);
            results.add(new KeyValue<>(sentence.toString(), prediction));
        }
        return results;
    }
}
