package KafkaConsumer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Arrays;
import EventClass.Conveyor;
import EventClass.Motor;
import com.google.gson.Gson;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;
import KafkaProducer.KafkaProducer;


public final class KafkaJsonConsumer {

    final static String brokers = "localhost:9092,localhost:9093";
    final static String conveyorTopic = "conveyor_events";
    final static String motorTopic = "motor_events";

    public static void main(String[] args) {

        // Create context with 2 second batch interval
        SparkConf sparkConf = new SparkConf().setAppName("manufacturer.KafKaConsumer").setMaster("local[4]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(3));

        HashSet<String> conveyorTopicsSet = new HashSet<String>(Arrays.asList(conveyorTopic.split(",")));
        HashSet<String> motorTopicsSet = new HashSet<String>(Arrays.asList(motorTopic.split(",")));

        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> conveyorKafkaStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                conveyorTopicsSet
        );

        JavaPairInputDStream<String, String> motorKafkaStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                motorTopicsSet
        );


        JavaDStream<Conveyor> deseriConveyor = conveyorKafkaStream.map(s -> s._2()).map(s -> new Gson().fromJson(s, Conveyor.class));
        JavaDStream<Motor> deseriMotor = motorKafkaStream.map(s -> s._2()).map(s -> new Gson().fromJson(s, Motor.class));


        deseriConveyor.filter(s -> s.getSpeed() < 200 || s.getSpeed() > 230).foreachRDD(rdd -> {
            rdd.collect().forEach(s -> System.out.println(s));
            return null;
        });

        deseriConveyor.filter(s -> s.getSpeed() < 175).foreachRDD(rdd -> {
            rdd.collect().forEach(s -> new KafkaProducer("localhost:9093").publish("Conveyor_alert", s));
            return null;
        });

        deseriMotor.filter(s -> s.getTemp() > 90).foreachRDD(rdd -> {
            rdd.collect().forEach(s -> System.out.println(s));
            return null;
        });



        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }
}
