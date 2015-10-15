package KafkaProducer;

import EventClass.Motor;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by qingl on 10/14/15.
 */
public class MotorEventRunnable implements Runnable {

    private final String name;
    private final KafkaProducer kafkaProducer;

    MotorEventRunnable (String name, KafkaProducer kafkaProducer) {
        this.name = name;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void run() {
        Random randomno = new Random();

        Motor motorEvent;

        while (true) {

            motorEvent = new Motor()
                    .newBuilder()
                    .setMotorId("1")
                    .setConveyorId("1")
                    .setSpeed(ThreadLocalRandom.current().nextDouble(15.0, 22.0))
                    .setTemp(ThreadLocalRandom.current().nextDouble(45.0, 80.0))
                    .setTimestamp(System.currentTimeMillis())
                    .setVibration(randomno.nextDouble())
                    .build();

            kafkaProducer.publish("motor_events", motorEvent);

            System.out.println("Motor " + name + " sent event!");

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
