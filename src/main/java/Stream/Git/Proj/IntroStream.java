package Stream.Git.Proj;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class IntroStream {
    public static void main(String[] args) {
        private static final DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        public static void main(String[] args) {
            StreamsBuilder builder = new StreamsBuilder();
            Properties config = new Properties();
            config.put(StreamsConfig.APPLICATION_ID_CONFIG, "intro-stream");
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "alireza:9092");
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
            config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "1");

            KStream<String, Long> rawString =  builder.stream("user-amount-input-topic-4",
                    Consumed.with(Serdes.String(), Serdes.Long()));

//       KTable<Windowed<String>, Long> aggRawString = rawString.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
//                .windowedBy(TimeWindows.of(Duration.ofMinutes(10)).grace(Duration.ofMinutes(1)))
//                .aggregate(
//                        () -> 0L,
//                        (key, aggValue, newValue) -> aggValue + newValue,
//                        Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("StoreValues")
//                        .withValueSerde(Serdes.Long())
//                );

            rawString.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                    .windowedBy(SessionWindows.with(Duration.ofMinutes(10)))
                    .aggregate(
                            () -> 0L,
                            (key, aggValue, newValue) -> aggValue + newValue,
                            (key, aggValue, newValue) -> aggValue + newValue,
                            Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("SessionStoreAgg")
                                    .withValueSerde(Serdes.Long())
                    );

            KTable<Windowed<String>, Long> sessionizedAggregatedStream = rawString.groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                    .windowedBy(SessionWindows.with(Duration.ofMinutes(10)))
                    .aggregate(
                            () -> 0L, /* initializer */
                            (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
                            (aggKey, leftAggValue, rightAggValue) -> leftAggValue + rightAggValue, /* session merger */
                            Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("sessionized-aggregated-stream-store") /* state store name */
                                    .withValueSerde(Serdes.Long())); /* serde for aggregate value */


            /////////////////////Test flatMap ///////////////////////////////////////

//        KStream<String, Integer> transformed = rawString.flatMap(
//                // Here, we generate two output records for each input record.
//                // We also change the key and value types.
//                // Example: (345L, "Hello") -> ("HELLO", 1000), ("hello", 9000)
//                (key, value) -> {
//                    List<KeyValue<String, Integer>> result = new LinkedList<>();
//                    result.add(KeyValue.pair(value.toString().toUpperCase(), 1000));
//                    result.add(KeyValue.pair(value.toString().toLowerCase(), 9000));
//                    return result;
//                }
//        );
            KStream<String, IntroImpl> mapRawString = rawString
                    .map((key, value)-> KeyValue.pair(key.toLowerCase(), value))
                    .map((key, value) -> {
                        IntroImpl II = new IntroImpl();
                        Date dd = new Date();
                        II.setAmount(value);
                        II.setDate(sdf.format(dd));
                        return KeyValue.pair(key, II);
                    });
            mapRawString.peek((key, value)-> {
                System.out.println(key+" "+ value.getDate()+" "+ value.getAmount());
            });
            mapRawString.selectKey((key, value) -> value.getDate())
                    .groupByKey();
//        KGroupedStream<String, Long> groupedByDate = mapRawString
//                .groupBy((key, value) -> KeyValue.pair(value.getDate(), value.getAmount()),
//                        Grouped.with(Serdes.String(), Serdes.Long()));


            KStream<String, Long> transformTest = rawString.flatMap(
                    (key, value)->{
                        List<KeyValue<String, Long>> lstUserTransaction = new ArrayList<>();
                        lstUserTransaction.add(KeyValue.pair(key, value + 2L));
                        return lstUserTransaction;

                    }
            );

            transformTest.foreach((key, value)-> System.out.println(key+" "+ value));

            KStream<String, Long> [] user_amount = rawString
                    .filter((key, value)-> value > 0)
                    .branch((key, value) -> value <=100,
                            (key, value) -> value > 100 & value <= 1000,
                            (key, value) -> value > 1000);
//        user_amount[0].foreach((key,value)-> {
//            System.out.println(key +" => "+ value);
//            System.out.println("user-amount[0]");
//        });
//        user_amount[1].foreach((key, value) -> {
//            System.out.println(key +" => "+ value);
//            System.out.println("user-amount[1]");
//        });
//        user_amount[2].foreach((key,value)->{
//            System.out.println(key +" => "+ value);
//            System.out.println("user-amount[2]");
//        });

            KGroupedStream<String, Long>
                    groupedStream_user_amount_zero =
                    user_amount[0].groupByKey();
            //KGroupedStream<String, Integer> groupedTable = user_amount[0].groupBy((key, value)-> key, Grouped.with(Serdes.String(), Serdes.Integer()));
            KGroupedStream<String, Long>
                    groupedStream_user_amount_one =
                    user_amount[1]
                            .groupByKey();
            KGroupedStream<String, Long>
                    groupedStream_user_amount_two =
                    user_amount[2]
                            .groupByKey();

            KTable<String, Long> user_amount_branch_zero =
                    groupedStream_user_amount_zero
                            .reduce((aggValue, newValue)-> aggValue + newValue);
            KTable<String, Long> user_amount_branch_one =
                    groupedStream_user_amount_one
                            .reduce((aggValue, newValue) -> aggValue + newValue);
            KTable<String, Long> user_amount_branch_two =
                    groupedStream_user_amount_two
                            .reduce((aggValue, newValue)-> aggValue + newValue);

            // Now I want to group user and the number of transaction above 1000 they have
            KTable<String, Long> countHighValueTransaction = user_amount[2]
                    .groupByKey()
                    .count(Materialized.as("highvaluetransactions"))
                    .filter((key,value)-> value > 5);
            // Also the total number of transaction and their values in each branch

            // Now i want to identify those guys that has a transaction above 1000 and the amount of
            // money in their account is less than two times of their transaction.


//        KTable<String, Long> user_amount_branch_zero = user_amount[0]
//                .groupByKey()
//                .reduce((aggValue, newValue) -> aggValue + newValue)
//                .mapValues(value-> value + 2);

//        KTable<String, Long> user_amount_branch_one =  user_amount[1]
//               .groupByKey()
//               .reduce((aggValue, newValue)-> aggValue + newValue)
//               .mapValues(value -> value + 3); //.to("output-mid-interest", Produced.with(Serdes.String(), Serdes.Double()));
//        KTable<String, Long> user_amount_branch_two = user_amount[2]
//                .groupByKey()
//                .reduce((aggValue, newValue)-> aggValue + newValue)
//                .mapValues(value-> value + 5);
            user_amount_branch_zero.toStream().foreach((key, value)-> System.out.println(key + " total:" + value));
            user_amount_branch_one.toStream().foreach((key, value)-> System.out.println(key + " total:" + value));
            user_amount_branch_two.toStream().foreach((key, value)-> System.out.println(key + " total:" + value));
//
            user_amount_branch_zero.toStream()
                    .to("user-amount-output", Produced.with(Serdes.String(), Serdes.Long()));
            user_amount_branch_one.toStream()
                    .to("user-amount-output", Produced.with(Serdes.String(), Serdes.Long()));
            user_amount_branch_two.toStream()
                    .to("user-amount-output", Produced.with(Serdes.String(), Serdes.Long()));
//        GlobalKTable<String, String> potentialFraudUsers = builder
//                .globalTable("fraud-user", Mater

            KafkaStreams streams = new KafkaStreams(builder.build(), config);
            streams.start();
            System.out.println(streams.toString());
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


            // (oldValue, newValue)-> oldValue + newValue, (oldValue, newValue)-> oldValue - newValue, "reduce-store").mapValues(value-> Integer.parseInt(value) * 0.02).to("output-low-interest", Produced.with(Serdes.String(), Serdes.Double()));

        }
    }
}
