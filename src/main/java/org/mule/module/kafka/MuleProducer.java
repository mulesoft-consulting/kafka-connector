package org.mule.module.kafka;

import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

public class MuleProducer {

	private KafkaProducer m_producer;
	private Properties m_properties;
	private int m_port;
	
	public MuleProducer(Properties properties) {
		initialize(properties);
	}
	
	public void send(String topic, Object key, Object message, long events) {
		
        KafkaProducer producer = getProducer();
 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               producer.send(new ProducerRecord(topic, key, message));
        }
	}

	public void shutdown() {
		getProducer().close();
	}
	
	protected void initialize(Properties properties) {
		
		if (properties.getProperty("key.serializer") == null || 
				properties.getProperty("value.serializer") == null) {
			
			properties.put("key.serializer", 
					"org.apache.kafka.common.serialization.StringSerializer");
			properties.put("value.serializer", 
					"org.apache.kafka.common.serialization.StringSerializer");
		}
		
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
