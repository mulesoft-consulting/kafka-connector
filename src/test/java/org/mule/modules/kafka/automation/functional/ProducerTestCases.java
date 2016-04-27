package org.mule.modules.kafka.automation.functional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.kafka.automation.functional.exception.FuntionalTestException;
import java.util.List;

/**
 * Created by bogdan.ilies on 20.04.2016.
 */
public class ProducerTestCases extends KafkaAbstractTestCases {

    private static final String TOPIC_NAME_TEMPLATE = "mule-test-topic-%s";
    private static final String TEST_MESSAGE_PAYLOAD = "test message";
    private static final String TEST_MESSAGE_KEY = "test-key";

    private String testTopicName;

    @Before
    public void setUp() throws FuntionalTestException {
        testTopicName = String.format(TOPIC_NAME_TEMPLATE, System.currentTimeMillis());
        createNewTestTopic(testTopicName);
        initializeConsumerGroupSource(testTopicName, 1);
    }

    @After
    public void tearDown() throws FuntionalTestException {
        deleteCreatedTopic(testTopicName);
    }

    @Test
    public void testProducer() throws FuntionalTestException {
        getConnector().producer(testTopicName, TEST_MESSAGE_KEY, TEST_MESSAGE_PAYLOAD, 1);
        List<Object> messages = collectSourceMessages(CONSUMER_GROUP_SOURCE_NAME, 60000);
        validateProducedMessages(messages);
    }

    private void validateProducedMessages(List<Object> messages) {
        Assert.assertNotNull("No messsage found.", messages);
        Assert.assertEquals("One message expected to be retrieved", 1, messages.size());
        String messagePayload = new String((byte[])messages.get(0));
        Assert.assertEquals("Invalid payload retrieved.", TEST_MESSAGE_PAYLOAD, messagePayload);
    }

}
