package org.mule.modules.kafka.automation.functional;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.jetbrains.annotations.NotNull;
import org.mule.modules.kafka.KafkaConnector;
import org.mule.modules.kafka.automation.functional.exception.FuntionalTestException;
import org.mule.tools.devkit.ctf.junit.AbstractTestCase;
import scala.collection.Map;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

/**
 * Created by bogdan.ilies on 20.04.2016.
 */
public class KafkaAbstractTestCases extends AbstractTestCase<KafkaConnector> {

    protected static final String CONSUMER_GROUP_SOURCE_NAME = "consumer";

    public KafkaAbstractTestCases() {
        super(KafkaConnector.class);
    }

    protected void deleteCreatedTopic(String topicName) throws FuntionalTestException {
        ZkUtils zkUtils = getZkUtils();
        try {
            AdminUtils.deleteTopic(zkUtils, topicName);
        } finally {
            zkUtils.close();
        }
    }

    protected void createNewTestTopic(String topicName) throws FuntionalTestException {
        ZkUtils zkUtils = getZkUtils();
        try {
            AdminUtils.createTopic(zkUtils, topicName, 1, 1, new Properties());
            Map<String, Properties> map = AdminUtils.fetchAllTopicConfigs(zkUtils);
            System.out.println("topic metadata list:\n" + map);
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

    protected void initializeConsumerSource(String topicName, Integer partitions) throws FuntionalTestException {
        Object[] consumerGroupParams = new Object[] { null, topicName, partitions};
        try {
            getDispatcher().initializeSource(CONSUMER_GROUP_SOURCE_NAME, consumerGroupParams);
        } catch (Throwable throwable) {
            throw new FuntionalTestException("Unable to initialize consumer source.", throwable);
        }
    }

    protected void shutdownConsumerSource() throws FuntionalTestException {
        try {
            getDispatcher().shutDownSource(CONSUMER_GROUP_SOURCE_NAME);
        } catch (Throwable throwable) {
            throw new FuntionalTestException("Unable to shutdown consumer source.", throwable);
        }
    }

    protected List<Object> collectSourceMessages(String sourceName, long timeOutMillis) throws FuntionalTestException {
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
        } while (messages.isEmpty() && totalTime <= timeOutMillis );
        return messages;
    }
}
