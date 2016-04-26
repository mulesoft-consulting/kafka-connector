/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.mule.api.callback.SourceCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuleConsumerGroup {
	private static Logger logger = LoggerFactory.getLogger(MuleConsumerGroup.class);
	private ConsumerConnector m_connector;
	private ExecutorService m_executor;
	
	public MuleConsumerGroup(Properties props) {
		startup(props);
	}
	
	public void run(final SourceCallback callback, String topic, int threadCount) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
	    topicCountMap.put(topic, new Integer(threadCount));
	    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = 
	    		m_connector.createMessageStreams(topicCountMap);
	    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
	 
	    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

	    setExecutor(executor);
	    
	    for (final KafkaStream stream : streams) {
		  executor.submit(new Runnable() {
			  public void run() {
		    	ConsumerIterator<byte[], byte[]> it = stream.iterator();
		    	try {
		    		while (it.hasNext()) {
		    			callback.process(it.next().message());
		    		}
		    	} catch (Throwable e) {
		    		logger.error("ERROR", e);
		    	}
		    	logger.info("done");
		    }
		  });
	    }
	    
	    while (true) {
	    	if (executor.isTerminated()) break;
	    }
	}
	
	public boolean isConnected() {
		return getConnector() != null;
	}
	
	public void shutdown() {
		ExecutorService executor = getExecutor();
		ConsumerConnector connector = getConnector();
		
		if (executor != null) {
			executor.shutdown();
		}
		
		try {
			if (!executor.awaitTermination(Integer.MAX_VALUE, 
					TimeUnit.MILLISECONDS)) {
				logger.warn("Threadpool did not terminate cleanly.");
			}
		} catch (Exception e) {
			logger.error("Threadpool did not terminate cleanly:", e);
		}
		
		logger.info("all threads terminated");
		if (connector != null) {
			connector.shutdown();
		}
		logger.info("consumer group shutdown");
	}
	
	protected void startup(Properties props) {
		setConnector(kafka.consumer.Consumer.createJavaConsumerConnector(
				new ConsumerConfig(props)));
	}
	
	protected ConsumerConnector getConnector() {
		return m_connector;
	}

	protected void setConnector(ConsumerConnector connector) {
		this.m_connector = connector;
	}

	protected ExecutorService getExecutor() {
		return m_executor;
	}

	protected void setExecutor(ExecutorService executor) {
		this.m_executor = executor;
	}
}
