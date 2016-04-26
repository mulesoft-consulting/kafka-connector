package org.mule.modules.kafka.automation.functional;

import org.mule.modules.kafka.KafkaConnector;
import org.mule.tools.devkit.ctf.junit.AbstractTestCase;

/**
 * Created by bogdan.ilies on 20.04.2016.
 */
public class KafkaAbstractTestCases extends AbstractTestCase<KafkaConnector> {

    public KafkaAbstractTestCases() {
        super(KafkaConnector.class);
    }
}
