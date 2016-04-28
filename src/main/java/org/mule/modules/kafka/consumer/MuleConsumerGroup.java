/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.kafka.consumer;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.WakeupException;
import org.jetbrains.annotations.Nullable;
import org.mule.api.callback.SourceCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MuleConsumerGroup {

    private static final int POLLING_TIMEOUT_MS = 1000;
    private static Logger logger = LoggerFactory.getLogger(MuleConsumerGroup.class);
	private ExecutorService executorService;
    private List<ConsumerGroupTask> consumerGroupTasks;
    private KafkaConsumer consumer;
	
	public MuleConsumerGroup(Properties props) {
		startup(props);
	}
	
	public void run(final SourceCallback callback, String topic, int threadCount) {
        assignPartitionsToConsumer(topic, threadCount);
        seekToBeginning();
        launchListeningTasks(callback, threadCount, POLLING_TIMEOUT_MS);
	}

    private void seekToBeginning() {
        consumer.seekToBeginning();
    }

    private void launchListeningTasks(final SourceCallback callback, int threadCount, final long pollingTimeoutMs) {
        logger.debug("Rising {} threads for processing messages.", threadCount);
        executorService = Executors.newFixedThreadPool(threadCount);

        ConsumerGroupTask consumerGroupTask = null;
        for (int i = 0; i < threadCount; i++) {
            consumerGroupTask = new ConsumerGroupTask(callback, pollingTimeoutMs);
            executorService.execute(consumerGroupTask);
            consumerGroupTasks.add(consumerGroupTask);
        }
    }

    private void assignPartitionsToConsumer(String topic, int threadCount) {
        List<TopicPartition> partitionsToReadFrom = new ArrayList<TopicPartition>();
        for (int i = 0; i < threadCount; i++) {
            partitionsToReadFrom.add(new TopicPartition(topic, i));
        }
        consumer.assign(partitionsToReadFrom);
    }

	public void shutdown() {
        stopRunningTasks();
        closeConsumer();
        shutdownExecutorService();
    }

    private void shutdownExecutorService() {
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.MILLISECONDS)) {
                    logger.warn("Threadpool did not terminate cleanly.");
                }
                logger.info("All threads terminated.");
            } catch (Exception e) {
                logger.error("Threadpool did not terminate cleanly.", e);
            }
        }
    }

    private void closeConsumer() {
        logger.info("Closing Kafka consumer.");
        consumer.close();
        logger.info("Kafka consumer closed.");
    }

    private void stopRunningTasks() {
        for (ConsumerGroupTask consumerGroupTask : consumerGroupTasks) {
            consumerGroupTask.stop();
        }
    }

    private void startup(Properties props) {
        consumerGroupTasks = new ArrayList<ConsumerGroupTask>();
        consumer = new KafkaConsumer(props);
    }

    private class ConsumerGroupTask implements Runnable {

        private SourceCallback callback;

        private long pollingTimeoutMs;
        private volatile boolean running;

        public ConsumerGroupTask(SourceCallback callback, long pollingTimeoutMs) {
            this.callback = callback;
            this.pollingTimeoutMs = pollingTimeoutMs;
        }

        public void run() {
            markAsRunning();
            messagesListenerLoop();
        }

        private void messagesListenerLoop() {
            while (running) {
                ConsumerRecords<Object, Object> consumerRecords = pollNextChunkOfMessages();
                if (consumerRecords != null) {
                    processGottenRecords(consumerRecords);
                }
            }
            logger.debug("Task finished its execution.");
        }

        private void processGottenRecords(ConsumerRecords<Object, Object> consumerRecords) {
            for (ConsumerRecord consumerRecord : consumerRecords) {
                try {
                    logger.debug("Passing message with key: {}, offset: {}, partition: {} for processing.", consumerRecord.key(), consumerRecord.offset(),
                            consumerRecord.partition());
                    callback.process(consumerRecord.value());
                } catch (Exception e) {
                    logger.error("Unable to fed message into source callback...", e);
                }
            }
        }

        @Nullable private ConsumerRecords pollNextChunkOfMessages() {
            ConsumerRecords consumerRecords = null;
            logger.debug("Polling for messages with a timeout of {}.", pollingTimeoutMs);
            try {
                consumerRecords = consumer.poll(pollingTimeoutMs);
                logger.debug("Got {} messages.", consumerRecords.count());
            } catch (AuthorizationException e) {
                logger.error("Consumer not authorized.", e);
                stop();
            } catch (InvalidOffsetException e) {
                logger.error("Invalid offset is requested. Task won't be able to recover so it will be stopped.", e);
                stop();
            } catch (WakeupException e) {
                logger.warn("Task was forcibly woken up.", e);
            } catch (KafkaException e) {
                logger.warn("Task got unknown excetion from server. Task won't be able to recover so it will be stopped.", e);
                stop();
            }
            return consumerRecords;
        }

        private void markAsRunning() {
            running = true;
        }

        private void stop() {
            logger.debug("Stopping task...");
            running = false;
        }

    }
}
