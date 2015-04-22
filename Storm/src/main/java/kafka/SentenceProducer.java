package kafka;

import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SentenceProducer {

  public static final String TOPIC = "sentences";
  
  public static final String[] SENTENCES = new String[] { "That was the worst movie I have ever seen",
      "I love you and you are a good person", "Hadoop is the coolest program ever",
      "You did a nice job singing in the choir yesterday!", "not a good day.", "good are things are happening. lol", "I am so mad",
      "I am so rich", "I hate brownies. They are too rich", "I love brownies. They are so rich", "Rainbows make me want to cry",
      "I love to watch rainbows - they make me happy", "I hate coffee", "I love coffee", "You make me frown", "You make me smile" };

  public static final String[] CATEGORIES = new String[] { "Politics", "Sports", "Home", "Work" };

  public static void main(String... args) throws InterruptedException {
    long events = 10000;
    if (args.length > 0) {
      events = Long.parseLong(args[0]);
    }
    Random rnd = new Random();

    Properties props = new Properties();
    props.put("metadata.broker.list", "node1:9092,node2:9092,node3:9092,node4:9092");
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");

    ProducerConfig config = new ProducerConfig(props);
    Producer<String, String> producer = new Producer<String, String>(config);

    try {
      for (long nEvents = 0; nEvents < events; nEvents++) {
        String sentence = SENTENCES[rnd.nextInt(SENTENCES.length)];
        String category = CATEGORIES[rnd.nextInt(CATEGORIES.length)];
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("sentences", category, sentence);
        producer.send(data);
      }
    }
    finally {
      producer.close();
    }
  }
}
