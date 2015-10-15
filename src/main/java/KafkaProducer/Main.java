package KafkaProducer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by qingl on 10/12/15.
 */
public class Main {

    static final KafkaProducer kf = new KafkaProducer("localhost:9092");

    public static void main(String[] args) {
        ExecutorService es = Executors.newCachedThreadPool();

        es.execute(new MotorEventRunnable("1", kf));
        es.execute(new MotorEventRunnable("2", kf));
        es.execute(new MotorEventRunnable("3", kf));
        es.execute(new MotorEventRunnable("4", kf));
        es.execute(new MotorEventRunnable("5", kf));
        es.execute(new MotorEventRunnable("6", kf));
        es.execute(new MotorEventRunnable("7", kf));
        es.execute(new MotorEventRunnable("8", kf));
        es.execute(new MotorEventRunnable("9", kf));
        es.execute(new MotorEventRunnable("10", kf));
        es.execute(new ConveyorEventRunnable("1",kf));

        es.shutdown();

    }
}
