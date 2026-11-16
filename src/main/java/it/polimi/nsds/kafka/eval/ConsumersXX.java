package it.polimi.nsds.kafka.eval;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

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


            final HashMap<String, Integer> humidities =

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);


            // TODO: implement the processing logic
            while (true) {
                // TODO: implement the processing logic
            }
        }
    }

    private static class Reactor {
        private final String serverAddr;
        private final String consumerGroupId;

        private static final String inputTopic = "alerts";
        private static final String outputTopic = "processedAlerts";

        // TODO: add attributes if needed

        // TODO: add arguments if necessary
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

            // TODO: add properties if needed

            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

            // Producer
            final Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddr);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            // TODO: add properties if needed

            final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

            // TODO: implement the processing logic
            while (true) {
                // TODO: implement the processing logic
            }
        }
    }

    private class Window {
        private Deque<Integer> window = new ArrayDeque<>();

        public void add(Integer newValue) {
            int nullSafeValue = 0;
            if (newValue != null) nullSafeValue = newValue;

            if (window.size() >= 3) window.removeLast();
            window.addFirst(newValue);
        }

        public int windowSum() {
            int sum = 0;
            for (Integer val: window) {
                sum += val;
            }

            return sum/window.size();
        }
    }
}
