/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.kafka;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.mule.api.MuleContext;
import org.mule.api.callback.SourceCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuleConsumerGroup {
	private static Logger logger = LoggerFactory.getLogger(MuleConsumerGroup.class);
	private ExecutorService m_executor;
    private KafkaConsumer<byte[], byte[]> consumer;
	
	public MuleConsumerGroup(Properties props) {
        consumer = new KafkaConsumer<byte[], byte[]>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
        consumer.seekToBeginning();
		startup(props);
	}
	
	public void run(final SourceCallback callback, String topic, int threadCount) {
        List<TopicPartition> partitionsToReadFrom = new ArrayList<TopicPartition>();
        for (int i = 0; i < threadCount; i++) {
            partitionsToReadFrom.add(new TopicPartition(topic, i));
        }
        consumer.assign(partitionsToReadFrom);

        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        setExecutor(executorService);

        for (int i = 0; i < 10; i++) {
            executorService.submit(new Runnable() {

                public void run() {
                    ConsumerRecords<byte[], byte[]> consumerRecords = consumer.poll(1000);
                    for (ConsumerRecord<byte[], byte[]> consumerRecord : consumerRecords) {
                        try {
                            callback.process(consumerRecord.value());
                        } catch (Exception e) {
                            logger.error("Unable to fed message into source callback...", e);
                        }
                    }
                }
            });
        }
//		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//	    topicCountMap.put(topic, new Integer(threadCount));
//	    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
//	    		m_connector.createMessageStreams(topicCountMap);
//	    List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
//
//	    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
//
//	    setExecutor(executor);
//
//	    for (final KafkaStream stream : streams) {
//		  executor.submit(new Runnable() {
//			  public void run() {
//		    	ConsumerIterator<byte[], byte[]> it = stream.iterator();
//		    	try {
//		    		while (it.hasNext()) {
//		    			callback.process(it.next().message());
//		    		}
//		    	} catch (Throwable e) {
//		    		logger.error("ERROR", e);
//		    	}
//		    	logger.info("done");
//		    }
//		  });
//	    }
//
//	    while (true) {
//	    	if (executor.isTerminated()) break;
//	    }
	}
	
	public boolean isConnected() {
		return true;//getConnector() != null;
	}
	
	public void shutdown() {
        consumer.close();
        logger.info("Kafka consumer closed.");

        ExecutorService executor = getExecutor();
		if (executor != null) {
			executor.shutdown();
            try {
                if (!executor.awaitTermination(Integer.MAX_VALUE,
                        TimeUnit.MILLISECONDS)) {
                    logger.warn("Threadpool did not terminate cleanly.");
                }
                logger.info("All threads terminated.");
            } catch (Exception e) {
                logger.error("Threadpool did not terminate cleanly.", e);
            }
		}

		logger.info("Consumer group shutted down.");
	}
	
	protected void startup(Properties props) {
//		setConnector(kafka.consumer.Consumer.createJavaConsumerConnector(
//				new ConsumerConfig(props)));
	}
	
//	protected ConsumerConnector getConnector() {
//		return m_connector;
//	}

//	protected void setConnector(ConsumerConnector connector) {
//		this.m_connector = connector;
//	}

	protected ExecutorService getExecutor() {
		return m_executor;
	}

	protected void setExecutor(ExecutorService executor) {
		this.m_executor = executor;
	}
}
