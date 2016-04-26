/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.kafka;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.mule.api.callback.SourceCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuleSimpleConsumer {

	private static Logger logger = LoggerFactory.getLogger(MuleSimpleConsumer.class);
	private List<String> m_replicaBrokers = new ArrayList<String>();
	private List<String> m_seedBrokers = null;
	private int m_port;
	
	public MuleSimpleConsumer(List<String> brokers, int port) {
		m_seedBrokers = brokers;
		m_port = port;
	}
	
	public void run(SourceCallback callback, long maxReads, String topic, int partition) throws Exception {
		// find the meta data about the topic and partition we are interested in
		//
		PartitionMetadata metadata = findLeader(m_seedBrokers, m_port, topic, partition);
		if (metadata == null) {
			logger.warn("Can't find metadata for Topic and Partition. Exiting");
			return;
		}
		if (metadata.leader() == null) {
			logger.warn("Can't find Leader for Topic and Partition. Exiting");
			return;
		}
		String leadBroker = metadata.leader().host();
		String clientName = "Client_" + topic + "_" + partition;
		
		SimpleConsumer consumer = new SimpleConsumer(leadBroker, m_port, 100000, 64 * 1024, clientName);
		long readOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);

		int numErrors = 0;
		while (maxReads > 0) {
			if (consumer == null) {
				consumer = new SimpleConsumer(leadBroker, m_port, 100000, 64 * 1024, clientName);
			}
			FetchRequest req = new FetchRequestBuilder()
					.clientId(clientName)
					.addFetch(topic, partition, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
					.build();
			FetchResponse fetchResponse = consumer.fetch(req);
			if (fetchResponse.hasError()) {
				numErrors++;
				// Something went wrong!
				short code = fetchResponse.errorCode(topic, partition);
				logger.warn("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
				if (numErrors > 5) break;
				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
					// We asked for an invalid offset. For simple case ask for the last element to reset
					readOffset = getLastOffset(consumer,topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
					continue;
				}
				consumer.close();
				consumer = null;
				leadBroker = findNewLeader(leadBroker, topic, partition, m_port);
				continue;
			}
			numErrors = 0;

			long numRead = 0;
			for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
				long currentOffset = messageAndOffset.offset();
				if (currentOffset < readOffset) {
					logger.warn("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
					continue;
				}
				readOffset = messageAndOffset.nextOffset();
				ByteBuffer payload = messageAndOffset.message().payload();
				
				byte[] bytes = new byte[payload.limit()];
				payload.get(bytes);
				String s_payload = new String(bytes, "UTF-8");
				logger.info(String.valueOf(messageAndOffset.offset()) + ": " + s_payload);
				
				callback.process(s_payload);
				
				numRead++;
				maxReads--;
			}

			if (numRead == 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {}
			}
		}
		if (consumer != null) {
			consumer.close();
		}
	}

	public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
			long whichTime, String clientName) {
		TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
		Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
		requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
		kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
				requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
		OffsetResponse response = consumer.getOffsetsBefore(request);

		if (response.hasError()) {
			logger.error("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition) );
			return 0;
		}
		
		long[] offsets = response.offsets(topic, partition);
		return offsets[0];
	}
	
	private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
		for (int i = 0; i < 3; i++) {
			boolean goToSleep = false;
			PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition);
			if (metadata == null) {
				goToSleep = true;
			} else if (metadata.leader() == null) {
				goToSleep = true;
			} else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
				// first time through if the leader hasn't changed give ZooKeeper a second to recover
				// second time, assume the broker did recover before failover, or it was a non-Broker issu
				//
				goToSleep = true;
			} else {
				return metadata.leader().host();
			}
			if (goToSleep) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException ie) {
				}
			}
		}
		logger.error("Unable to find new leader after Broker failure. Exiting");
		throw new Exception("Unable to find new leader after Broker failure. Exiting");
	}
	
	private PartitionMetadata findLeader(List<String> seedBrokers, int port, String topic, int partition) {
		PartitionMetadata returnMetaData = null;
		loop:
			for (String seed : seedBrokers) {
				SimpleConsumer consumer = null;
				try {
					consumer = new SimpleConsumer(seed, port, 100000, 64*1024, topic);
					List<String> topics = Collections.singletonList(topic);
					TopicMetadataRequest req = new TopicMetadataRequest(topics);
					kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
					
					List<TopicMetadata> metaData = resp.topicsMetadata();
					for (TopicMetadata item : metaData) {
						for (PartitionMetadata part : item.partitionsMetadata()) {
							if (part.partitionId() == partition) {
								returnMetaData = part;
								break loop;
							}
						}
					}
				} catch (Exception e) {
					logger.error("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
							+ ", " + partition + "] Reason: " + e);
				} finally {
					if (consumer != null) consumer.close();
				}
			}
		if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.BrokerEndPoint replica : returnMetaData.replicas()) {
                    m_replicaBrokers.add(replica.host());
            }
		}
		return returnMetaData;
	}
}
