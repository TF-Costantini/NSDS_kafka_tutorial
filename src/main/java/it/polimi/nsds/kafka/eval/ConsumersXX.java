package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;

// Group number:
// Group members:

// How many partitions can the topics "temperature" and "humidity" have?
    // They can be [1, N) partitions as they are processed by Monitor by Key (roomID) this already ensures that all processors within the group receive coherent set of keys.

// Is there any relation between the number of partitions in "temperature" and "humidity"?
    // They must be equals otherwises they might get assigned to different consumers and we'd never be able to compare them in the IF block

// Is it possible to have more than one instance of Monitor? Yes
// If so, what is the relation between their group id?
    //They must be the same. Otherwise we'd double process and duplicate alerts.

// How many partitions can the topic "alerts" have?
    //[1, N) as they are also keyed by roomID.

// Is it possible to have more than one instance of Reactor? Yes
// If so, what is the relation between their group id?
    //Based on our implementation they must have the same groupId

// How many partitions can the topic "processedAlerts" have?
    //[1, N) as they are also keyed on roomId.

// Which additional topics, if any, do you use? How many partitions can they have?

public class ConsumersXX {
    public static void main(String[] args) {
        String serverAddr = "localhost:9092";
        int stage = Integer.parseInt(args[0]);
        String groupId = args[1];
        // TODO: add arguments if necessary
        switch (stage) {
            case 1:
                new Monitor(serverAddr, groupId).execute();
                break;
            case 2:
                new Reactor(serverAddr, groupId).execute();
                break;
            default:
                System.err.println("Wrong stage");
        }
    }

    // Simulates the first reaction of the system to an alert (provided, do not modify)
    private static void reaction1(String room, String alert) {
        System.out.println("[reaction1] room=" + room + " " + alert);
    }

    // Simulates the second reaction of the system to an alert (provided, do not modify)
    private static void reaction2(String room, String alert) {
        System.out.println("[reaction2] room=" + room + " " + alert);
    }

    private static class Monitor {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String temperaturesTopic = "temperature";
        private static final String humiditiesTopic = "humidity";
        private static final String outputTopic = "alerts";

        private static final int windowSize = 3;
        private static final double temperatureThreshold = 28;
        private static final double humidityThreshold = 40;

        // TODO: add attributes if necessary

        // TODO: add arguments if necessary
        public Monitor(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(true));
            consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);
            // Deliberate choice, if I register to an old topic I want to consume only the latest stuff not weeks old messages for this problem domain.
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

            KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Arrays.asList(temperaturesTopic, humiditiesTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);


            final HashMap<String, Window> humidities = new HashMap<>();
            final HashMap<String, Window> temps = new HashMap<>();
            while (true) {
                final ConsumerRecords<String, Integer> records = consumer.poll(Duration.of(5, ChronoUnit.MINUTES));
                for (final ConsumerRecord<String, Integer> thisRecord: records) {
                    String topic = thisRecord.topic();
                    String key = thisRecord.key();
                    Integer value = thisRecord.value();

                    Window tempWindow, humWindow; //temp value always replaced
                    humidities.putIfAbsent(key, new Window());
                    humWindow = humidities.get(key);
                    temps.putIfAbsent(key, new Window());
                    tempWindow = temps.get(key);

                    if (topic.equals(humiditiesTopic)) {
                        humWindow.addValue(value);
                    } else if (topic.equals(temperaturesTopic)) {
                        tempWindow.addValue(value);
                    }

                    if (!humWindow.isFull() || !tempWindow.isFull()) continue;

                    double humSum = humWindow.windowSum();
                    System.out.println("Key: " + key + "\thumidity Total: " +  humSum);
                    double tempSum = tempWindow.windowSum();
                    System.out.println("Key: " + key + "\ttemp Total: " + tempSum);

                    if (tempSum > temperatureThreshold && humSum < humidityThreshold) {
                        System.out.println("OVER THRESHOLD -> humidity: " +  humSum + "\ttemp: " + tempSum);
                        producer.send(new ProducerRecord<>(outputTopic, key, "OVER THRESHOLD"));
                    }
                }
            }
        }
    }

    private static class Reactor {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String inputTopic = "alerts";
        private static final String reactionsTopic = "reactions";
        private static final String outputTopic = "processedAlerts";

        private static final String reaction1 = "reaction1";
        private static final String reaction2 = "reaction2";


        public Reactor(String serverAddr, String consumerGroupId) {
            this.serverAddr = serverAddr;
            this.consumerGroupId = consumerGroupId;
        }

        public void execute() {
            // Consumer
            final Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);

            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Arrays.asList(inputTopic));

            final Properties consumerProps2 = new Properties();
            consumerProps2.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            consumerProps2.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId + "-reactions");

            consumerProps2.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps2.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ReactionDeserializer.class.getName());
            consumerProps2.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, String.valueOf(false));
            consumerProps2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            KafkaConsumer<String, Reaction> consumer2 = new KafkaConsumer<>(consumerProps2);
            consumer2.subscribe(Arrays.asList(reactionsTopic));

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            // Producer2
            final Properties producerProps2 = new Properties();
            producerProps2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ReactionSerializer.class.getName());

            final KafkaProducer<String, Reaction> producer2 = new KafkaProducer<>(producerProps2);

            while (true) {
                final ConsumerRecords<String, String> inputRecords = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                for (final ConsumerRecord<String, String> thisRecord: inputRecords) {
                    String key = thisRecord.key();
                    String value = thisRecord.value();

                    producer2.send(new ProducerRecord<>(reactionsTopic, key, new Reaction(reaction1, value)));
                    consumer.commitSync(Collections.singletonMap(new TopicPartition(thisRecord.topic(), thisRecord.partition()), new OffsetAndMetadata(thisRecord.offset() + 1)));
                }

                final ConsumerRecords<String, Reaction> reactionRecords = consumer2.poll(Duration.of(100, ChronoUnit.MILLIS));
                for (final ConsumerRecord<String, Reaction> reactRecord: reactionRecords) {
                    String key = reactRecord.key();
                    Reaction value = reactRecord.value();

                    if (value.reaction.equals(reaction1)) {
                        reaction1(key, value.msg);
                        producer2.send(new ProducerRecord<>(reactionsTopic, key, new Reaction(reaction2, value.msg)));
                    } else if (value.reaction.equals(reaction2)) {
                        reaction2(key, value.msg);
                        producer.send(new ProducerRecord<>(outputTopic, key, value.msg));
                        System.out.println("FINAL MESSAGE room: " + key + " msg:" + value.msg);
                    }
                    consumer2.commitSync(Collections.singletonMap(new TopicPartition(reactRecord.topic(), reactRecord.partition()), new OffsetAndMetadata(reactRecord.offset() + 1)));
                }
            }
        }

    }

    private record Reaction(String reaction, String msg) {}

    public static class ReactionSerializer implements Serializer<Reaction> {
        @Override
        public byte[] serialize(String topic, Reaction data) {
            if (data == null) return null;
            return (data.reaction() + ";" + data.msg()).getBytes(StandardCharsets.UTF_8);
        }
    }

    public static class ReactionDeserializer implements Deserializer<Reaction> {
        @Override
        public Reaction deserialize(String topic, byte[] data) {
            if (data == null) return null;
            String[] parts = new String(data, StandardCharsets.UTF_8).split(";", 2);
            return new Reaction(parts[0], parts[1]);
        }
    }


    private static class Window {
        private Deque<Integer> window = new ArrayDeque<>();

        public Window() {}

        public void addValue(Integer newValue) {
            int nullSafeValue = 0;
            if (newValue != null) nullSafeValue = newValue;

            if (window.size() == 3) window.removeLast();
            window.addFirst(nullSafeValue);
        }

        public boolean isFull() {
            return window.size() == 3;
        }

        public double windowSum() {
            int sum = 0;
            for (Integer val: window) {
                sum += val;
            }

            return (double) sum / window.size();
        }
    }
}
