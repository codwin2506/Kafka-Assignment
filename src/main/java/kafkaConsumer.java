import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.io.FileWriter;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class kafkaConsumer {

    public static void main(String[] args) {
        ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }
    public static void consumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("user");
        kafkaConsumer.subscribe(topics);
        try{
            // Message1

            while (true){
                ConsumerRecords<String, String> records = kafkaConsumer.poll(10);

                for (ConsumerRecord<String, String> record: records){
                    System.out.println(String.format(record.value()));
                    // method that would be writing this value to a file.

                    try {
                        FileWriter myWriter = new FileWriter("filename.txt");
                        myWriter.write(record.value());
                        myWriter.close();
                        //System.out.println("Successfully wrote to the file.");
                    } catch (IOException e) {
                        System.out.println("An error occurred.");
                        e.printStackTrace();
                    }


                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }
}

class ConsumerListener implements Runnable {


    @Override
    public void run() {
        kafkaConsumer.consumer();
    }
}

