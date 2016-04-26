package org.mule.modules.kafka.automation.runner;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.mule.modules.kafka.KafkaConnector;
import org.mule.modules.kafka.automation.functional.ConsumerGroupTestCases;
import org.mule.modules.kafka.automation.functional.ProducerTestCases;
import org.mule.tools.devkit.ctf.mockup.ConnectorTestContext;

/**
 * Created by bogdan.ilies on 20.04.2016.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        ProducerTestCases.class,
//        ConsumerGroupTestCases.class
})
public class FunctionalTestSuite {

    @BeforeClass
    public static void initialiseSuite() {
        ConnectorTestContext.initialize(KafkaConnector.class);
    }

    @AfterClass
    public static void shutdownSuite() throws Exception {
        ConnectorTestContext.shutDown();
    }
}
