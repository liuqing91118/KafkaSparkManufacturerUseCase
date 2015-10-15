package KafkaProducer;

import EventClass.Conveyor;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by qingl on 10/14/15.
 */
public class ConveyorEventRunnable implements Runnable {

    private final String name;
    private final KafkaProducer kafkaProducer;

    ConveyorEventRunnable (String name, KafkaProducer kafkaProducer) {
        this.name = name;
        this.kafkaProducer = kafkaProducer;
    }

    @Override
    public void run() {
        Random randomno = new Random();

        Conveyor conveyorEvent;

        while (true) {

            conveyorEvent = new Conveyor()
                    .newBuilder()
                    .setTimestamp(System.currentTimeMillis())
                    .setConveyorId(name)
                    .setSpeed(ThreadLocalRandom.current().nextDouble(150.0, 220.0))
                    .setVibration(randomno.nextDouble())
                    .setLoad(randomno.nextDouble())
                    .build();

            kafkaProducer.publish("conveyor_events", conveyorEvent);

            System.out.println("Conveyor " + name + " sent event!");

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
