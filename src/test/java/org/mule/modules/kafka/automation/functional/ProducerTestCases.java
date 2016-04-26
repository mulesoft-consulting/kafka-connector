package org.mule.modules.kafka.automation.functional;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mule.modules.kafka.automation.functional.exception.FuntionalTestException;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Created by bogdan.ilies on 20.04.2016.
 */
public class ProducerTestCases extends KafkaAbstractTestCases {

    private static final String TOPIC_NAME_TEMPLATE = "mule-test-topic-%s";
    private static final String CONSUMER_GROUP_SOURCE_NAME = "consumerGroup";
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
        //deleteCreatedTopic(testTopicName);
    }

    private void deleteCreatedTopic(String topicName) throws FuntionalTestException {
        ZkUtils zkUtils = getZkUtils();
        try {
            AdminUtils.deleteTopic(zkUtils, topicName);
        } finally {
            zkUtils.close();
        }
    }

    private void createNewTestTopic(String topicName) throws FuntionalTestException {
        ZkUtils zkUtils = getZkUtils();
        try {
            AdminUtils.createTopic(zkUtils, topicName, 1, 1, new Properties());
        } finally {
            zkUtils.close();
        }

    }

    @NotNull private ZkUtils getZkUtils() throws FuntionalTestException {
        ZkClient zkClient = initializeZkClient();
        ZkConnection zkConnection = initializeZkConnection();
        return new ZkUtils(zkClient, zkConnection, false);
    }

    private ZkConnection initializeZkConnection() throws FuntionalTestException {
        String zookeeperUrl = getAutomationCredentialProperty("zookeeperUrl");
        return new ZkConnection(zookeeperUrl);
    }

    private String getAutomationCredentialProperty(String propertyName) throws FuntionalTestException {
        Properties automationCredentialProperties = getFunctionalTestAutomationCredentialProperties();
        return automationCredentialProperties.getProperty(System.getProperty("activeconfiguration") + "." + propertyName);
    }

    private ZkClient initializeZkClient() throws FuntionalTestException {
        String zookeeperUrl = getAutomationCredentialProperty("zookeeperUrl");
        return new ZkClient(zookeeperUrl, 10000, 10000, ZKStringSerializer$.MODULE$);
    }

    private Properties getFunctionalTestAutomationCredentialProperties() throws FuntionalTestException {
        Properties automationCredentialsproperties = new Properties();
        try {
            automationCredentialsproperties.load(getAutomationCredentialPropertiesResourceStream());
        } catch (IOException e) {
            throw new FuntionalTestException("Unable to load automation credential properties.", e);
        }
        return automationCredentialsproperties;
    }

    private InputStream getAutomationCredentialPropertiesResourceStream() {
        return getClass().getClassLoader().getResourceAsStream(System.getProperty("automation-credentials.properties"));
    }

    private void initializeConsumerGroupSource(String topicName, Integer partitions) throws FuntionalTestException {
        Object[] consumerGroupParams = new Object[] { null, topicName, partitions};
        try {
            getDispatcher().initializeSource(CONSUMER_GROUP_SOURCE_NAME, consumerGroupParams);
        } catch (Throwable throwable) {
            throw new FuntionalTestException("Unable to initialize consumer group source.", throwable);
        }
    }

    @Test
    public void testProducer() throws FuntionalTestException {
        getConnector().producer(testTopicName, TEST_MESSAGE_KEY, TEST_MESSAGE_PAYLOAD, 1);
        List<Object> messages = collectSourceMessages(CONSUMER_GROUP_SOURCE_NAME, 10000);
        validateProducedMessages(messages);
    }

    private void validateProducedMessages(List<Object> messages) {
        Assert.assertNotNull("No messsage found.", messages);
        Assert.assertEquals("One message expected to be retrieved", 1, messages.size());
        String messagePayload = String.valueOf(messages.get(0));
        Assert.assertEquals("Invalid payload retrieved.", TEST_MESSAGE_PAYLOAD, messagePayload);
    }

    private List<Object> collectSourceMessages(String sourceName, long timeOutMillis) throws FuntionalTestException {
        long startTime = System.currentTimeMillis();
        List<Object> messages = null;
        long totalTime = 0;
        do {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new FuntionalTestException("Thread interrupted while waiting for messages from source.", e);
            }
            messages = getDispatcher().getSourceMessages(sourceName);
            long iterationEnd = System.currentTimeMillis();
            totalTime = iterationEnd - startTime;
        } while (!messages.isEmpty() || totalTime >= timeOutMillis );
        return messages;
    }

}
