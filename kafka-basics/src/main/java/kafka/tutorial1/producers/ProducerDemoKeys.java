package kafka.tutorial1.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        for(Integer i=0; i<10; i++) {

            String topic = "first_topic";
            String value = Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key);
            //send data
//            Output for my execution is :
//            Key: id_0	Partition: 1
//            Key: id_1	Partition: 0
//            Key: id_2	Partition: 2
//            Key: id_3	Partition: 0
//            Key: id_4	Partition: 2
//            Key: id_5	Partition: 2
//            Key: id_6	Partition: 0
//            Key: id_7	Partition: 2
//            Key: id_8	Partition: 1
//            Key: id_9	Partition: 2

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is successfully send or exception is thrown
                    if (e != null) {
                        //sending record was unsuccessful
                        logger.error("Error while producing ", e);

                    } else {
                        logger.info("Received new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offsets: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    }
                }
            }).get(); //block the .send() to make it synchronous but don't do this in prod!
        }

        //flush and close
        producer.flush();
        producer.close();
    }
}
