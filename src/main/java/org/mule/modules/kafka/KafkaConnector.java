/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.kafka;

import java.util.Properties;

import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.annotations.licensing.RequiresEnterpriseLicense;
import org.mule.api.callback.SourceCallback;
import org.mule.api.callback.StopSourceCallback;
import org.mule.modules.kafka.config.ConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cloud Connector
 * 
 * @author MuleSoft, Inc.
 */
@RequiresEnterpriseLicense(allowEval = true)
@Connector(name = "ApacheKafka", friendlyName = "Apache Kafka")
public class KafkaConnector {
	private static Logger logger = LoggerFactory.getLogger(KafkaConnector.class);

	@Config
	ConnectorConfig config;
	
	public ConnectorConfig getConfig() {
		return config;
	}
	public void setConfig(ConnectorConfig config) {
		this.config = config;
	}

	@Source(name = "SimpleConsumer", friendlyName = "SimpleConsumer")
	public void simpleConsumer(SourceCallback callback, String topic, int partition, long maxReads) {
		MuleSimpleConsumer consumer = new MuleSimpleConsumer(config.getParsedBrokerList(), config.getBrokerPort());
		
		try {
			consumer.run(callback, maxReads, topic, partition);
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}

	@Source(name = "ConsumerGroup", friendlyName = "ConsumerGroup")
	public void consumerGroup(SourceCallback callback, String topic, int partitions) {
		Properties props = config.getZookeeperProperties();
		
		if (props == null) {
			logger.error("Missing Zookeeper Connection Properties");
		} else {
			MuleConsumerGroup consumer = new MuleConsumerGroup(props);
			consumer.run(callback, topic, partitions);
			consumer.shutdown();
		}
	}
	
	@Processor(name = "Producer", friendlyName = "Producer")
	public void producer(String topic, String key, String message, long events) {
		MuleProducer producer = new MuleProducer(config.getProducerProperties());
		
		producer.send(topic, key, message, events);
		producer.shutdown();
	}
}
