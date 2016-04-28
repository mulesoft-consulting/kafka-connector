/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.kafka;

import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.mule.api.annotations.Config;
import org.mule.api.annotations.Connector;
import org.mule.api.annotations.Processor;
import org.mule.api.annotations.Source;
import org.mule.api.annotations.licensing.RequiresEnterpriseLicense;
import org.mule.api.callback.SourceCallback;
import org.mule.api.callback.StopSourceCallback;
import org.mule.modules.kafka.config.ConnectorConfig;
import org.mule.modules.kafka.consumer.MuleConsumerGroup;
import org.mule.modules.kafka.producer.MuleProducer;
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

	@Source(friendlyName = "ConsumerGroup")
	public StopSourceCallback consumer(SourceCallback callback, String topic, int partitions) {
		validateConsumerProperties();
		Properties consumerProperties = getConsumerProperties();

		final MuleConsumerGroup consumer = new MuleConsumerGroup(consumerProperties);
		consumer.run(callback, topic, partitions);

		return new StopSourceCallback() {

			public void stop() throws Exception {
				consumer.shutdown();
			}
		};
	}

	private Properties getConsumerProperties() {
		Properties properties = config.getZookeeperProperties();
		if (!properties.containsKey("key.deserializer")) {
			properties.setProperty("key.deserializer", StringDeserializer.class.getName());
		}
		if (!properties.containsKey("value.deserializer")) {
			properties.setProperty("value.deserializer", StringDeserializer.class.getName());
		}
		return properties;
	}

	@NotNull private void validateConsumerProperties() {
		Properties props = config.getZookeeperProperties();

		if (props == null) {
			logger.error("Missing consumer connection properties.");
			throw new IllegalArgumentException("Missing consumer connection properties.");
		}
	}

	@Processor(name = "Producer", friendlyName = "Producer")
	public void producer(String topic, String key, String message) {
		validateProducerProperties();
		MuleProducer producer = new MuleProducer(getProducerProperties());
		
		producer.send(topic, key, message);
		producer.shutdown();
	}

	@NotNull private void validateProducerProperties() {
		Properties props = config.getProducerProperties();

		if (props == null) {
			logger.error("Missing consumer connection properties.");
			throw new IllegalArgumentException("Missing consumer connection properties.");
		}
	}

	private Properties getProducerProperties() {
		Properties properties =  config.getProducerProperties();
		if (!properties.containsKey("key.serializer")) {
			properties.put("key.serializer",
					"org.apache.kafka.common.serialization.StringSerializer");
		}
		if (!properties.containsKey("value.serializer")) {
			properties.put("value.serializer",
					"org.apache.kafka.common.serialization.StringSerializer");
		}
		return properties;
	}
}
