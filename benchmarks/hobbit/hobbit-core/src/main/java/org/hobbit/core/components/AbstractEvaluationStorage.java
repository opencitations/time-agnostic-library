/**
 * This file is part of core.
 *
 * core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with core.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.hobbit.core.components;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.commons.io.IOUtils;
import org.apache.jena.ext.com.google.common.collect.Lists;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.data.RabbitQueue;
import org.hobbit.core.data.Result;
import org.hobbit.core.data.ResultPair;
import org.hobbit.core.rabbit.DataHandler;
import org.hobbit.core.rabbit.DataReceiver;
import org.hobbit.core.rabbit.DataReceiverImpl;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * This abstract class implements basic functions that can be used to implement
 * a task generator.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public abstract class AbstractEvaluationStorage extends AbstractPlatformConnectorComponent
        implements ResponseReceivingComponent, ExpectedResponseReceivingComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEvaluationStorage.class);

    public static final String RECEIVE_TIMESTAMP_FOR_SYSTEM_RESULTS_KEY = "HOBBIT_RECEIVE_TIMESTAMP_FOR_SYSTEM_RESULTS";

    /**
     * If a request contains this iterator ID, a new iterator is created and its
     * first result as well as its Id are returned.
     */
    public static final byte NEW_ITERATOR_ID = -1;
    /**
     * The empty response that is sent if an error occurs.
     */
    private static final byte[] EMPTY_RESPONSE = new byte[0];
    /**
     * Default value of the {@link #maxParallelProcessedMsgs} attribute.
     */
    private static final int DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES = 50;

    /**
     * Mutex used to wait for the termination signal.
     */
    private Semaphore terminationMutex = new Semaphore(0);
    /**
     * The maximum number of incoming messages of a single queue that are processed
     * in parallel. Additional messages have to wait.
     */
    private final int maxParallelProcessedMsgs;
    /**
     * Iterators that have been started.
     */
    protected List<Iterator<? extends ResultPair>> resultPairIterators = Lists.newArrayList();
    /**
     * The incoming queue from the task generator.
     */
    protected DataReceiver taskResultReceiver;
    /**
     * The incoming queue from the system.
     */
    protected DataReceiver systemResultReceiver;
    /**
     * The incoming queue from the evaluation module.
     */
    protected RabbitQueue evalModule2EvalStoreQueue;
    /**
     * Channel on which the acknowledgements are send.
     */
    protected Channel ackChannel = null;

    /**
     * Constructor using the {@link #DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES}=
     * {@value #DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES}.
     */
    public AbstractEvaluationStorage() {
        this(DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES);
    }

    /**
     * Constructor setting the maximum number of messages processed in parallel.
     *
     * @param maxParallelProcessedMsgs
     *            The maximum number of incoming messages of a single queue that are
     *            processed in parallel. Additional messages have to wait.
     */
    public AbstractEvaluationStorage(int maxParallelProcessedMsgs) {
        this.maxParallelProcessedMsgs = maxParallelProcessedMsgs;
        defaultContainerType = Constants.CONTAINER_TYPE_DATABASE;
    }

    @Override
    public void init() throws Exception {
        super.init();

        String queueName = configuration.getString(Constants.TASK_GEN_2_EVAL_STORAGE_QUEUE_NAME_KEY,
                Constants.TASK_GEN_2_EVAL_STORAGE_DEFAULT_QUEUE_NAME);
        taskResultReceiver = DataReceiverImpl.builder().maxParallelProcessedMsgs(maxParallelProcessedMsgs)
                .queue(incomingDataQueueFactory, generateSessionQueueName(queueName)).dataHandler(new DataHandler() {
                    @Override
                    public void handleData(byte[] data) {
                        ByteBuffer buffer = ByteBuffer.wrap(data);
                        String taskId = RabbitMQUtils.readString(buffer);
                        LOGGER.trace("Received from task generator {}.", taskId);
                        byte[] taskData = RabbitMQUtils.readByteArray(buffer);
                        long timestamp = buffer.getLong();
                        receiveExpectedResponseData(taskId, timestamp, taskData);
                    }
                }).build();

        queueName = configuration.getString(Constants.SYSTEM_2_EVAL_STORAGE_QUEUE_NAME_KEY,
                Constants.SYSTEM_2_EVAL_STORAGE_DEFAULT_QUEUE_NAME);
        final boolean receiveTimeStamp = configuration.getBoolean(RECEIVE_TIMESTAMP_FOR_SYSTEM_RESULTS_KEY, false,
                LOGGER);
        final String ackExchangeName = generateSessionQueueName(Constants.HOBBIT_ACK_EXCHANGE_NAME);
        systemResultReceiver = DataReceiverImpl.builder().maxParallelProcessedMsgs(maxParallelProcessedMsgs)
                .queue(incomingDataQueueFactory, generateSessionQueueName(queueName)).dataHandler(new DataHandler() {
                    @Override
                    public void handleData(byte[] data) {
                        ByteBuffer buffer = ByteBuffer.wrap(data);
                        String taskId = RabbitMQUtils.readString(buffer);
                        LOGGER.trace("Received from system {}.", taskId);
                        byte[] responseData = RabbitMQUtils.readByteArray(buffer);
                        long timestamp = receiveTimeStamp ? buffer.getLong() : System.currentTimeMillis();
                        receiveResponseData(taskId, timestamp, responseData);
                        // If we should send acknowledgments (and there was no
                        // error until now)
                        if (ackChannel != null) {
                            try {
                                ackChannel.basicPublish(ackExchangeName, "", null, RabbitMQUtils.writeString(taskId));
                            } catch (IOException e) {
                                LOGGER.error("Error while sending acknowledgement.", e);
                            }
                            LOGGER.trace("Sent ack {}.", taskId);
                        }
                    }
                }).build();

        queueName = configuration.getString(Constants.EVAL_MODULE_2_EVAL_STORAGE_QUEUE_NAME_KEY,
                Constants.EVAL_MODULE_2_EVAL_STORAGE_DEFAULT_QUEUE_NAME);
        evalModule2EvalStoreQueue = getFactoryForIncomingDataQueues()
                .createDefaultRabbitQueue(generateSessionQueueName(queueName));
        evalModule2EvalStoreQueue.channel.basicConsume(evalModule2EvalStoreQueue.name, true,
                new DefaultConsumer(evalModule2EvalStoreQueue.channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties,
                            byte[] body) throws IOException {
                        byte response[] = null;
                        // get iterator id
                        ByteBuffer buffer = ByteBuffer.wrap(body);
                        if (buffer.remaining() < 1) {
                            response = EMPTY_RESPONSE;
                            LOGGER.error("Got a request without a valid iterator Id. Returning emtpy response.");
                        } else {
                            byte iteratorId = buffer.get();

                            // get the iterator
                            Iterator<? extends ResultPair> iterator = null;
                            if (iteratorId == NEW_ITERATOR_ID) {
                                // create and save a new iterator
                                iteratorId = (byte) resultPairIterators.size();
                                LOGGER.info("Creating new iterator #{}", iteratorId);
                                resultPairIterators.add(iterator = createIterator());
                            } else if ((iteratorId < 0) || iteratorId >= resultPairIterators.size()) {
                                response = EMPTY_RESPONSE;
                                LOGGER.error("Got a request without a valid iterator Id (" + Byte.toString(iteratorId)
                                        + "). Returning emtpy response.");
                            } else {
                                iterator = resultPairIterators.get(iteratorId);
                            }
                            if ((iterator != null) && (iterator.hasNext())) {
                                ResultPair resultPair = iterator.next();
                                Result result = resultPair.getExpected();
                                byte expectedResultData[], expectedResultTimeStamp[], actualResultData[],
                                        actualResultTimeStamp[];
                                // Make sure that the result is not null
                                if (result != null) {
                                    // Check whether the data array is null
                                    expectedResultData = result.getData() != null ? result.getData() : new byte[0];
                                    expectedResultTimeStamp = RabbitMQUtils.writeLong(result.getSentTimestamp());
                                } else {
                                    expectedResultData = new byte[0];
                                    expectedResultTimeStamp = RabbitMQUtils.writeLong(0);
                                }
                                result = resultPair.getActual();
                                // Make sure that the result is not null
                                if (result != null) {
                                    // Check whether the data array is null
                                    actualResultData = result.getData() != null ? result.getData() : new byte[0];
                                    actualResultTimeStamp = RabbitMQUtils.writeLong(result.getSentTimestamp());
                                } else {
                                    actualResultData = new byte[0];
                                    actualResultTimeStamp = RabbitMQUtils.writeLong(0);
                                }

                                response = RabbitMQUtils
                                        .writeByteArrays(
                                                new byte[] { iteratorId }, new byte[][] { expectedResultTimeStamp,
                                                        expectedResultData, actualResultTimeStamp, actualResultData },
                                                null);
                            } else {
                                response = new byte[] { iteratorId };
                            }
                        }
                        getChannel().basicPublish("", properties.getReplyTo(), null, response);
                    }
                });

        boolean sendAcks = configuration.getBoolean(Constants.ACKNOWLEDGEMENT_FLAG_KEY, false, LOGGER);
        if (sendAcks) {
            // Create channel for acknowledgements
            ackChannel = getFactoryForOutgoingCmdQueues().getConnection().createChannel();
            ackChannel.exchangeDeclare(generateSessionQueueName(Constants.HOBBIT_ACK_EXCHANGE_NAME), "fanout", false,
                    true, null);
        }
    }

    /**
     * Creates a new iterator that iterates over the response pairs.
     *
     * @return a new iterator or null if an error occurred
     */
    protected abstract Iterator<? extends ResultPair> createIterator();

    @Override
    public void run() throws Exception {
        sendToCmdQueue(Commands.EVAL_STORAGE_READY_SIGNAL);
        terminationMutex.acquire();
        taskResultReceiver.closeWhenFinished();
        systemResultReceiver.closeWhenFinished();
    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        // If this is the signal to start the data generation
        if (command == Commands.EVAL_STORAGE_TERMINATE) {
            // release the mutex
            terminationMutex.release();
        }
        super.receiveCommand(command, data);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(taskResultReceiver);
        IOUtils.closeQuietly(systemResultReceiver);
        IOUtils.closeQuietly(evalModule2EvalStoreQueue);
        if (ackChannel != null) {
            try {
                ackChannel.close();
            } catch (Exception e) {
                LOGGER.error("Error while trying to close the acknowledgement channel.", e);
            }
        }
        super.close();
    }
}
