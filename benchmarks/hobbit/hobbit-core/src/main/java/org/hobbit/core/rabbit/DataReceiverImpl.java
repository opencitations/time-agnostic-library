package org.hobbit.core.rabbit;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.hobbit.core.data.RabbitQueue;
import org.hobbit.utils.TerminatableRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Delivery;


/**
 * Implementation of the {@link DataReceiver} interface.
 * 
 * <p>
 * Use the internal {@link Builder} class for creating instances of the
 * {@link DataReceiverImpl} class. <b>Note</b> that the created
 * {@link DataReceiverImpl} will either use a given {@link RabbitQueue} or
 * create a new one. In both cases the receiver will become the owner of the
 * queue, i.e., if the {@link DataReceiverImpl} instance is closed the queue
 * will be closed as well.
 * </p>
 * <p>
 * Internally, the receiver uses an own thread to consume incoming messages.
 * These messages are forwarded to the given {@link DataHandler} instance.
 * <b>Note</b> that this forwarding is based on an {@link ExecutorService} the
 * called method {@link DataHandler#handleData(byte[])} should be thread safe
 * since it might be called in parallel.
 * </p>
 * <p>
 * The {@link DataReceiverImpl} owns recources that need to be freed if its work
 * is done. This can be achieved by closing the receiver. In most cases, this
 * should be done using the {@link #closeWhenFinished()} method which waits
 * until all incoming messages are processed and all streams are closed. Note
 * that using the {@link #close()} method leads to a direct shutdown of the
 * queue which could lead to data loss and threads getting stuck.
 * </p>
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class DataReceiverImpl implements DataReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataReceiverImpl.class);

    private static final int DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES = 50;

    protected RabbitQueue queue;
    private int errorCount = 0;
    private DataHandler dataHandler;
    private ExecutorService executor = null;
    private TerminatableRunnable receiverTask;
    private Thread receiverThread;

    protected DataReceiverImpl(RabbitQueue queue, DataHandler handler, int maxParallelProcessedMsgs)
            throws IOException {
        this.queue = queue;
        this.dataHandler = handler;
        QueueingConsumer consumer = new QueueingConsumer(queue.channel);
        queue.channel.basicConsume(queue.name, true, consumer);
        queue.channel.basicQos(maxParallelProcessedMsgs);
        executor = Executors.newFixedThreadPool(maxParallelProcessedMsgs);
        receiverTask = buildMsgReceivingTask(consumer);
        receiverThread = new Thread(receiverTask);
        receiverThread.start();
    }

    public DataHandler getDataHandler() {
        return dataHandler;
    }

    public synchronized void increaseErrorCount() {
        ++errorCount;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public RabbitQueue getQueue() {
        return queue;
    }

    protected ExecutorService getExecutor() {
        return executor;
    }

    /**
     * This method waits for the data receiver to finish its work and closes the
     * incoming queue as well as the internal thread pool after that.
     */
    public void closeWhenFinished() {
        receiverTask.terminate();
        // Try to wait for the receiver task to finish
        try {
            receiverThread.join();
        } catch (Exception e) {
            LOGGER.error("Exception while waiting for termination of receiver task. Closing receiver.", e);
        }
        // After the receiver task finished, no new tasks are added to the
        // executor. Now we can ask the executor to shut down.
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            LOGGER.error("Exception while waiting for termination. Closing receiver.", e);
        }
        close();
    }

    /**
     * A rude way to close the receiver. Note that this method directly closes the
     * incoming queue and only notifies the internal consumer to stop its work but
     * won't wait for the handler threads to finish their work.
     */
    public void close() {
        IOUtils.closeQuietly(queue);
        if ((executor != null) && (!executor.isShutdown())) {
            executor.shutdownNow();
        }
    }

    /**
     * Returns a newly created {@link Builder}.
     * 
     * @return a new {@link Builder} instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * This factory method creates a runnable task that uses the given consumer to
     * receive incoming messages.
     * 
     * @param consumer
     *            the consumer that can be used to receive messages
     * @return a Runnable instance that will handle incoming messages as soon as it
     *         will be executed
     */
    protected TerminatableRunnable buildMsgReceivingTask(QueueingConsumer consumer) {
        return new MsgReceivingTask(consumer);
    }

    /**
     * This factory method creates a runnable task that processes the given message.
     * 
     * @param delivery
     *            the message that should be processed
     * @return a Runnable instance that will process the message as soon as it will
     *         be executed
     */
    protected Runnable buildMsgProcessingTask(Delivery delivery) {
        return new MsgProcessingTask(delivery);
    }

    protected class MsgReceivingTask implements TerminatableRunnable {

        private QueueingConsumer consumer;
        private boolean runFlag = true;
        private boolean terminatedFlag = false;

        public MsgReceivingTask(QueueingConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            terminatedFlag = false;
            int count = 0;
            Delivery delivery = null;
            while (runFlag || (queue.messageCount() > 0) || (delivery != null)) {
                try {
                    delivery = consumer.getDeliveryQueue().poll(3000, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    if(getErrorCount() == 0) {
                        LOGGER.error("Exception while waiting for delivery.", e);
                    } else {
                        LOGGER.error("Exception while waiting for delivery. {}", e.getMessage());
                    }
                    increaseErrorCount();
                }
                if (delivery != null) {
                    executor.submit(buildMsgProcessingTask(delivery));
                    ++count;
                }
            }
            LOGGER.debug("Receiver task terminates after receiving {} messages.", count);
            terminatedFlag = true;
        }

        @Override
        public void terminate() {
            runFlag = false;
        }

        @Override
        public boolean isTerminated() {
            return terminatedFlag;
        }

    }

    protected class MsgProcessingTask implements Runnable {

        private Delivery delivery;

        public MsgProcessingTask(Delivery delivery) {
            this.delivery = delivery;
        }

        @Override
        public void run() {
            try {
                dataHandler.handleData(delivery.getBody());
            } catch (Throwable e) {
                LOGGER.error("Uncatched throwable inside the data handler.", e);
            }
        }

    }

    public static class Builder {

        protected static final String QUEUE_INFO_MISSING_ERROR = "There are neither a queue nor a queue name and a queue factory provided for the DataReceiver. Either a queue or a name and a factory to create a new queue are mandatory.";
        protected static final String DATA_HANDLER_MISSING_ERROR = "The necessary data handler has not been provided for the DataReceiver.";

        protected DataHandler dataHandler;
        protected RabbitQueue queue;
        protected String queueName;
        protected int maxParallelProcessedMsgs = DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES;
        protected RabbitQueueFactory factory;

        public Builder() {
        };

        /**
         * Sets the handler that is called if data is incoming.
         * 
         * @param dataHandler
         *            the handler that is called if data is incoming
         * @return this builder instance
         */
        public Builder dataHandler(DataHandler dataHandler) {
            this.dataHandler = dataHandler;
            return this;
        }

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
         * Method for providing the necessary information to create a queue if it has
         * not been provided with the {@link #queue(RabbitQueue)} method. Note that this
         * information is not used if a queue has been provided.
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
         * Sets the maximum number of incoming messages that are processed in parallel.
         * Additional messages have to wait in the queue.
         * 
         * @param maxParallelProcessedMsgs
         *            the maximum number of incoming messages that are processed in
         *            parallel
         * @return this builder instance
         */
        public Builder maxParallelProcessedMsgs(int maxParallelProcessedMsgs) {
            this.maxParallelProcessedMsgs = maxParallelProcessedMsgs;
            return this;
        }

        /**
         * Builds the {@link DataReceiverImpl} instance with the previously given
         * information.
         * 
         * @return The newly created DataReceiver instance
         * @throws IllegalStateException
         *             if the dataHandler is missing or if neither a queue nor the
         *             information needed to create a queue have been provided.
         * @throws IOException
         *             if an exception is thrown while creating a new queue or if the
         *             given queue can not be configured by the newly created
         *             DataReceiver. <b>Note</b> that in the latter case the queue will
         *             be closed.
         */
        public DataReceiverImpl build() throws IllegalStateException, IOException {
            if (dataHandler == null) {
                throw new IllegalStateException(DATA_HANDLER_MISSING_ERROR);
            }
            if (queue == null) {
                if ((queueName == null) || (factory == null)) {
                    throw new IllegalStateException(QUEUE_INFO_MISSING_ERROR);
                } else {
                    // create a new queue
                    queue = factory.createDefaultRabbitQueue(queueName);
                }
            }
            try {
                return new DataReceiverImpl(queue, dataHandler, maxParallelProcessedMsgs);
            } catch (IOException e) {
                IOUtils.closeQuietly(queue);
                throw e;
            }
        }
    }

}