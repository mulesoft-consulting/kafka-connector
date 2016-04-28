package org.mule.modules.kafka.automation.functional;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.kafka.automation.functional.exception.FuntionalTestException;

import java.util.List;

/**
 * Created by bogdan.ilies on 21.04.2016.
 */
public class ConsumerGroupTestCases extends KafkaAbstractTestCases {

    private static final String TOPIC_NAME_TEMPLATE = "mule-test-topic-%s";
    private static final String TEST_MESSAGE_PAYLOAD = "test-message";
    private static final String TEST_MESSAGE_KEY = "test-key";
    private String testTopicName;

    @Before
    public void setUp() throws FuntionalTestException {
        testTopicName = String.format(TOPIC_NAME_TEMPLATE, System.currentTimeMillis());
        createNewTestTopic(testTopicName);
        getConnector().producer(testTopicName, TEST_MESSAGE_KEY, TEST_MESSAGE_PAYLOAD);
    }

    @After
    public void tearDown() throws FuntionalTestException {
        deleteCreatedTopic(testTopicName);
        shutdownConsumerSource();
    }

    @Test
    public void testConsumerGroup() throws FuntionalTestException {
        initializeConsumerSource(testTopicName, 1);
        List<Object> consumedMessages = collectSourceMessages(testTopicName, 30000);
        validateProducedMessages(consumedMessages);
    }

    private void validateProducedMessages(List<Object> messages) {
        Assert.assertNotNull("No messsage found.", messages);
        Assert.assertEquals("One message expected to be retrieved", 1, messages.size());
        String messagePayload = new String((byte[])messages.get(0));
        Assert.assertEquals("Invalid payload retrieved.", TEST_MESSAGE_PAYLOAD, messagePayload);
    }

}
