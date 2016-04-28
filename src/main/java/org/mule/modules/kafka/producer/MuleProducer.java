/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.kafka.producer;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class MuleProducer {

	private KafkaProducer m_producer;
	private Properties m_properties;

	public MuleProducer(Properties properties) {
		initialize(properties);
	}
	
	public void send(String topic, Object key, Object message) {
		KafkaProducer producer = getProducer();
   		producer.send(new ProducerRecord(topic, key, message));
	}

	public void shutdown() {
		getProducer().close();
	}
	
	protected void initialize(Properties properties) {
		setProducer(new KafkaProducer(properties));
	}
	
	protected Properties getPoperties() {
		return m_properties;
	}

	protected void setProperties(Properties properties) {
		this.m_properties = properties;
	}

	protected KafkaProducer getProducer() {
		return m_producer;
	}

	protected void setProducer(KafkaProducer producer) {
		this.m_producer = producer;
	}
}
