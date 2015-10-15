package KafkaProducer;

import EventClass.Conveyor;
import EventClass.Motor;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by qingl on 10/12/15.
 */
public class Main {

    static final KafkaProducer kf = new KafkaProducer("localhost:9092");

    public static void main(String[] args) {
        Random randomno = new Random();

        Conveyor conveyorEvent = null;
        Motor motorEvent = null;

        while (true) {

            conveyorEvent = new Conveyor()
                    .newBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setConveyorId("1")
                    .setSpeed(ThreadLocalRandom.current().nextDouble(150.0, 220.0))
                    .setVibration(randomno.nextDouble())
                    .setLoad(randomno.nextDouble())
                    .build();

            motorEvent = new Motor()
                    .newBuilder()
                    .setMotorId("1")
                    .setConveyorId("1")
                    .setSpeed(ThreadLocalRandom.current().nextDouble(15.0, 22.0))
                    .setTemp(ThreadLocalRandom.current().nextDouble(45.0, 80.0))
                    .setTimestamp(System.currentTimeMillis())
                    .setVibration(randomno.nextDouble())
                    .build();

            kf.publish("conveyor_events", conveyorEvent);
            kf.publish("motor_events", motorEvent);

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
