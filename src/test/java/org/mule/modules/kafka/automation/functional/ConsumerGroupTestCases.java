package org.mule.modules.kafka.automation.functional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.kafka.automation.functional.exception.FuntionalTestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by bogdan.ilies on 21.04.2016.
 */
public class ConsumerGroupTestCases extends KafkaAbstractTestCases {

    private static final String TOPIC_NAME_TEMPLATE = "mule-test-topic-%s";
    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerGroupTestCases.class);
    private KafkaConsumer<String, String> consumer;
    private String testTopicName;
    private KafkaProducer<String, String> producer;

    @Before
    public void setUp() throws FuntionalTestException {
        testTopicName = String.format(TOPIC_NAME_TEMPLATE, System.currentTimeMillis());
        createNewTestTopic(testTopicName);
//        initializeConsumerGroupSource(testTopicName, 1);

        Properties consumerProperties = new Properties();
        Properties producerProperties = new Properties();
        try {
            consumerProperties.load(getClass().getClassLoader().getResourceAsStream("consumer.properties"));
            producerProperties.load(getClass().getClassLoader().getResourceAsStream("producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        consumer = new KafkaConsumer<String, String>(consumerProperties, new StringDeserializer(), new StringDeserializer());
        producer = new KafkaProducer<String, String>(producerProperties, new StringSerializer(), new StringSerializer());
    }

    @After
    public void tearDown() throws FuntionalTestException {
        consumer.close();
        deleteCreatedTopic(testTopicName);
    }

    @Test
    public void testConsumerGroup() {
        producer.send(new ProducerRecord<String, String>(testTopicName, 0, "test-key", "test-message"));
        consumer.assign(Arrays.asList(new TopicPartition(testTopicName, 0)));
        consumer.seekToBeginning();
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            LOGGER.debug("Got: {} records.", consumerRecords.count());
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                LOGGER.debug("messageKey: {} messagePayload: {}", consumerRecord.key(), consumerRecord.value());
            }
        }
    }

}
