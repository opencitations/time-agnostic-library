package org.hobbit.core.rabbit;

import java.io.IOException;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

import org.apache.commons.io.IOUtils;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.ConfirmListener;

/**
 * Implementation of the {@link DataSender} interface.
 * 
 * <p>
 * Use the internal {@link Builder} class for creating instances of the
 * {@link DataSenderImpl} class. <b>Note</b> that the created
 * {@link DataSenderImpl} will either use a given {@link RabbitQueue} or create
 * a new one. In both cases the receiver will become the owner of the queue,
 * i.e., if the {@link DataSenderImpl} instance is closed the queue will be
 * closed as well.
 * </p>
 * 
 * <p>
 * <b>Note</b> that choosing a message buffer size smaller than
 * {@link #DEFAULT_MESSAGE_BUFFER_SIZE}={@value #DEFAULT_MESSAGE_BUFFER_SIZE}
 * might lead the sender to get stuck since confirmations might be sent rarely
 * by the RabbitMQ broker.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class DataSenderImpl implements DataSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSenderImpl.class);

    private static final int DEFAULT_MESSAGE_BUFFER_SIZE = 1000;
    private static final int DEFAULT_DELIVERY_MODE = 2;

    private RabbitQueue queue;
    private final int deliveryMode;
    private final DataSenderConfirmHandler confirmHandler;

    protected DataSenderImpl(RabbitQueue queue, int deliveryMode, int messageConfirmBuffer) {
        this.queue = queue;
        this.deliveryMode = deliveryMode;

        if (messageConfirmBuffer > 0) {
            try {
                this.queue.channel.confirmSelect();
            } catch (Exception e) {
                LOGGER.error(
                        "Exception whily trying to enable confirms. The sender might work, but it won't guarantee that messages are received.");
                confirmHandler = null;
                return;
            }
            confirmHandler = new DataSenderConfirmHandler(messageConfirmBuffer);
            this.queue.channel.addConfirmListener(confirmHandler);
        } else {
            confirmHandler = null;
        }
    }

    @Override
    public void sendData(byte[] data) throws IOException {
        sendData(data, new BasicProperties.Builder());
    }

    protected void sendData(byte[] data, BasicProperties.Builder probBuilder) throws IOException {
        probBuilder.deliveryMode(deliveryMode);
        if (confirmHandler != null) {
            confirmHandler.sendDataWithConfirmation(probBuilder.build(), data);
        } else {
            sendData(probBuilder.build(), data);
        }
    }

    protected void sendData(BasicProperties properties, byte[] data) throws IOException {
        queue.channel.basicPublish("", queue.name, properties, data);
    }

    @Override
    public void closeWhenFinished() {
        // If we want to make sure that all messages are delivered we have to
        // wait until all messages are consumed
        if (confirmHandler != null) {
            try {
                confirmHandler.waitForConfirms();
            } catch (InterruptedException e) {
                LOGGER.warn(
                        "Exception while waiting for confirmations. It can not be guaranteed that all messages have been consumed.",
                        e);
            }
        }
        try {
            // Simply check whether the queue is empty. If the check is true
            // 5 times, we can assume that it is empty
            int check = 0;
            while (check < 5) {
                if (queue.messageCount() > 0) {
                    check = 0;
                } else {
                    ++check;
                }
                Thread.sleep(200);
            }
        } catch (AlreadyClosedException e) {
            LOGGER.info("The queue is already closed. Assuming that all messages have been consumed.");
        } catch (Exception e) {
            LOGGER.warn("Exception while trying to check whether all messages have been consumed. It will be ignored.",
                    e);
        }
        close();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(queue);
    }

    /**
     * Returns a newly created {@link Builder}.
     * 
     * @return a new {@link Builder} instance
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        protected static final String QUEUE_INFO_MISSING_ERROR = "There are neither a queue nor a queue name and a queue factory provided for the DataSender. Either a queue or a name and a factory to create a new queue are mandatory.";

        protected RabbitQueue queue;
        protected String queueName;
        protected RabbitQueueFactory factory;
        protected int messageConfirmBuffer = DEFAULT_MESSAGE_BUFFER_SIZE;
        protected int deliveryMode = DEFAULT_DELIVERY_MODE;

        public Builder() {
        };

        /**
         * Sets the queue that is used to receive data.
         * 
         * @param queue
         *            the queue that is used to receive data
         * @return this builder instance
         */
        public Builder queue(RabbitQueue queue) {
            this.queue = queue;
            return this;
        }

        /**
         * Method for providing the necessary information to create a queue if
         * it has not been provided with the {@link #queue(RabbitQueue)} method.
         * Note that this information is not used if a queue has been provided.
         * 
         * @param factory
         *            the queue factory used to create a queue
         * @param queueName
         *            the name of the newly created queue
         * @return this builder instance
         */
        public Builder queue(RabbitQueueFactory factory, String queueName) {
            this.factory = factory;
            this.queueName = queueName;
            return this;
        }

        /**
         * <p>
         * Sets the number of messages that are buffered while waiting for a
         * confirmation that they have been received by the broker. Note that if
         * the message buffer has reached is maximum size, the sender will block
         * until confirmations are received.
         * </p>
         * <p>
         * If the given message buffer size is {@code <1} the usage of
         * confirmation messages is turned off.
         * </p>
         * 
         * @param messageConfirmBuffer
         *            the size of the messages buffer
         * @return this builder instance
         */
        public Builder messageBuffer(int messageConfirmBuffer) {
            this.messageConfirmBuffer = messageConfirmBuffer;
            return this;
        }

        /**
         * Sets the delivery mode used for the RabbitMQ messages. Please have a
         * look into the RabbitMQ documentation to see the different meanings of
         * the values. By default, the sender uses
         * {@link DataSenderImpl#DEFAULT_DELIVERY_MODE}.
         * 
         * @param deliveryMode
         *            the delivery mode used for the RabbitMQ messages
         * @return this builder instance
         */
        public Builder deliveryMode(int deliveryMode) {
            this.deliveryMode = deliveryMode;
            return this;
        }

        /**
         * Builds the {@link DataReceiverImpl} instance with the previously
         * given information.
         * 
         * @return The newly created DataReceiver instance
         * @throws IllegalStateException
         *             if neither a queue nor the information needed to create a
         *             queue have been provided.
         * @throws IOException
         *             if an exception is thrown while creating a new queue.
         */
        public DataSenderImpl build() throws IllegalStateException, IOException {
            if (queue == null) {
                if ((queueName == null) || (factory == null)) {
                    throw new IllegalStateException(QUEUE_INFO_MISSING_ERROR);
                } else {
                    queue = factory.createDefaultRabbitQueue(queueName);
                }
            }
            return new DataSenderImpl(queue, deliveryMode, messageConfirmBuffer);
        }
    }

    protected static class Message {
        public BasicProperties properties;
        public byte[] data;

        public Message(BasicProperties properties, byte[] data) {
            this.properties = properties;
            this.data = data;
        }
    }

    protected class DataSenderConfirmHandler implements ConfirmListener {

        private final Semaphore maxBufferedMessageCount;
        private final SortedMap<Long, Message> unconfirmedMsgs = Collections
                .synchronizedSortedMap(new TreeMap<Long, Message>());
        private int successfullySubmitted = 0;

        public DataSenderConfirmHandler(int messageConfirmBuffer) {
            this.maxBufferedMessageCount = new Semaphore(messageConfirmBuffer);
        }

        public synchronized void sendDataWithConfirmation(BasicProperties properties, byte[] data) throws IOException {
            try {
                LOGGER.trace("{}\tavailable\t{}", DataSenderImpl.this.toString(),
                        maxBufferedMessageCount.availablePermits());
                maxBufferedMessageCount.acquire();
            } catch (InterruptedException e) {
                throw new IOException("Interrupted while waiting for free buffer to store the message before sending.",
                        e);
            }
            synchronized (unconfirmedMsgs) {
                sendData_unsecured(new Message(properties, data));
            }
        }

        private void sendData_unsecured(Message message) throws IOException {
            // Get ownership of the channel to make sure that nobody else is
            // using it while we get the next sequence number and send the next
            // data
            synchronized (queue.channel) {
                long sequenceNumber = queue.channel.getNextPublishSeqNo();
                LOGGER.trace("{}\tsending\t{}", DataSenderImpl.this.toString(), sequenceNumber);
                unconfirmedMsgs.put(sequenceNumber, message);
                try {
                    sendData(message.properties, message.data);
                } catch (IOException e) {
                    // the message hasn't been sent, remove it from the set
                    unconfirmedMsgs.remove(sequenceNumber);
                    maxBufferedMessageCount.release();
                    throw e;
                }
            }
        }

        @Override
        public void handleAck(long deliveryTag, boolean multiple) throws IOException {
            synchronized (unconfirmedMsgs) {
                if (multiple) {
                    // Remove all acknowledged messages
                    SortedMap<Long, Message> negativeMsgs = unconfirmedMsgs.headMap(deliveryTag + 1);
                    int ackMsgCount = negativeMsgs.size();
                    negativeMsgs.clear();
                    maxBufferedMessageCount.release(ackMsgCount);
                    successfullySubmitted += ackMsgCount;
                    LOGGER.trace("{}\tack\t{}+\t{}", DataSenderImpl.this.toString(), deliveryTag,
                            maxBufferedMessageCount.availablePermits());
                } else {
                    // Remove the message
                    unconfirmedMsgs.remove(deliveryTag);
                    ++successfullySubmitted;
                    maxBufferedMessageCount.release();
                    LOGGER.trace("{}\tack\t{}\t{}", DataSenderImpl.this.toString(), deliveryTag,
                            maxBufferedMessageCount.availablePermits());
                }
            }
        }

        @Override
        public void handleNack(long deliveryTag, boolean multiple) throws IOException {
            synchronized (unconfirmedMsgs) {
                LOGGER.trace("nack\t{}{}", deliveryTag, (multiple ? "+" : ""));
                if (multiple) {
                    // Resend all lost messages
                    SortedMap<Long, Message> negativeMsgs = unconfirmedMsgs.headMap(deliveryTag + 1);
                    Message messageToResend[] = negativeMsgs.values().toArray(new Message[negativeMsgs.size()]);
                    negativeMsgs.clear();
                    for (int i = 0; i < messageToResend.length; ++i) {
                        sendData_unsecured(messageToResend[i]);
                    }
                } else {
                    if (unconfirmedMsgs.containsKey(deliveryTag)) {
                        // send the lost message again
                        Message message = unconfirmedMsgs.remove(deliveryTag);
                        sendData_unsecured(message);
                    } else {
                        LOGGER.warn(
                                "Got a negative acknowledgement (nack) for an unknown message. It will be ignored.");
                    }
                }
            }
        }

        public void waitForConfirms() throws InterruptedException {
            while (true) {
                synchronized (unconfirmedMsgs) {
                    if (unconfirmedMsgs.size() == 0) {
                        LOGGER.trace("sent {} messages.", successfullySubmitted);
                        return;
                    }
                }
                Thread.sleep(200);
            }
        }

    }

}