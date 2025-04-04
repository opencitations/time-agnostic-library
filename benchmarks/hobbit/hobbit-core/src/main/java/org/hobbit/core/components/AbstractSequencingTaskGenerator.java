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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.hobbit.core.Constants;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * Extension of the {@link AbstractTaskGenerator} that offers methods to wait
 * for acknowledgements for single tasks. The workflow for waiting for a task is
 * <ul>
 * <li>generate a task and a task id (for the latter {@link #getNextTaskId()}
 * should be used)</li>
 * <li>set the task id which will be sent next using
 * {@link #setTaskIdToWaitFor(String)}</li>
 * <li>send the task to the system with
 * {@link #sendTaskToSystemAdapter(String, byte[])} and
 * {@link #sendTaskToEvalStorage(String, long, byte[])}</li>
 * <li>wait for the acknowledgement using {@link #waitForAck()}</li>
 * </ul>
 *
 * <p>
 * <b>Note</b> that the acknowledgements only work if the evaluation storage is
 * configured to send acknowledgements for the single tasks.
 * </p>
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public abstract class AbstractSequencingTaskGenerator extends AbstractTaskGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSequencingTaskGenerator.class);
    /**
     * Default timeout for the acknowledgement.
     */
    private static final long DEFAULT_ACK_TIMEOUT = 600000;
    /**
     * Timeout for the acknowledgement.
     */
    private long ackTimeout = DEFAULT_ACK_TIMEOUT;
    /**
     * Id of the task the generator is waiting for an acknowledgement.
     */
    private String seqTaskId = null;
    /**
     * Semaphore used to wait for the acknowledgement.
     */
    private Semaphore taskIdMutex = new Semaphore(0);
    /**
     * Channel on which the acknowledgments are received.
     */
    protected Channel ackChannel;

    public AbstractSequencingTaskGenerator() {
        // TODO remove this 1 from the constructor
        super(1);
    }
    // TODO reactivate this constructor
    // public AbstractSequencingTaskGenerator(int numberOfMessagesInParallel) {
    // super(numberOfMessagesInParallel);
    // }

    @Override
    public void init() throws Exception {
        super.init();
        // Create channel for incoming acknowledgments using the command
        // connection (not the data connection!)
        ackChannel = getFactoryForIncomingCmdQueues().getConnection().createChannel();
        String queueName = ackChannel.queueDeclare().getQueue();
        String exchangeName = generateSessionQueueName(Constants.HOBBIT_ACK_EXCHANGE_NAME);
        ackChannel.exchangeDeclare(exchangeName, "fanout", false, true, null);
        ackChannel.queueBind(queueName, exchangeName, "");
        Consumer consumer = new DefaultConsumer(ackChannel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                    byte[] body) throws IOException {
                try {
                    handleAck(body);
                } catch (Exception e) {
                    LOGGER.error("Exception while trying to handle incoming command.", e);
                }
            }
        };
        ackChannel.basicConsume(queueName, true, consumer);
        ackChannel.basicQos(1);
    }

    /**
     * Handles the acknowledgement messages.
     *
     * @param body
     *            the body of the acknowledgement message
     */
    protected void handleAck(byte[] body) {
        String ackTaskId = RabbitMQUtils.readString(body);
        LOGGER.trace("Received ack{}.", ackTaskId);
        // Make sure that the task id is not changed while we work with it
        synchronized (this) {
            if ((seqTaskId != null) && (seqTaskId.equals(ackTaskId))) {
                seqTaskId = null;
                taskIdMutex.release();
            }
        }
    }

    /**
     * Sends the given task with the given task id and data to the system and
     * blocks until an acknowledgement has been received for the task or the
     * timeout has been reached. If an information is needed which of these two
     * cases happened {@link #sendTaskToSystemAdapterInSequence(String, byte[])}
     * should be used.
     */
    @Override
    protected void sendTaskToSystemAdapter(String taskIdString, byte[] data) throws IOException {
        sendTaskToSystemAdapterInSequence(taskIdString, data);
    }

    /**
     * Sends the given task with the given task id and data to the system and
     * blocks until an acknowledgement has been received for the task or the
     * timeout has been reached. The return value shows which of these two cases
     * happened.
     *
     * @param taskIdString the task id
     * @param data the data of the task
     * @return <code>true</code> if the acknowledgement has been received,
     *         <code>false</code> if the timeout has been reached or the method
     *         has been interrupted.
     * @throws IOException
     *             if there is an error during the sending
     */
    protected boolean sendTaskToSystemAdapterInSequence(String taskIdString, byte[] data) throws IOException {
        // make sure that only one thread can send and wait for the mutex at the
        // same time
        synchronized (taskIdMutex) {
            synchronized (this) {
                this.seqTaskId = taskIdString;
            }
            super.sendTaskToSystemAdapter(taskIdString, data);
            return waitForAck();
        }
    }

    /**
     * Method to set the task id for which the task generator will wait when
     * calling {@link #waitForAck()}.
     *
     * @param taskId
     *            id of the task for which the acknowledgement should be
     *            received
     *
     * @deprecated It is not necessary anymore since its usage has been
     *             integrated into the
     *             {@link #sendTaskToSystemAdapterInSequence(String, byte[])}
     *             method.
     */
    @Deprecated
    protected void setTaskIdToWaitFor(String taskId) {
        this.seqTaskId = taskId;
    }

    /**
     * Method to wait for the acknowledgement of a task with the given Id.
     *
     * @return <code>true</code> if the acknowledgement has been received,
     *         <code>false</code> if the timeout has been reached or the method
     *         has been interrupted.
     */
    private boolean waitForAck() {
        LOGGER.trace("Waiting for ack{}.", seqTaskId);
        boolean ack = false;
        try {
            ack = taskIdMutex.tryAcquire(ackTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            LOGGER.info("Interrupted while waiting for acknowledgement.", e);
        }
        return ack;
    }

    /**
     * Setter for the maximum time the task generator waits for an
     * acknowledgement.
     *
     * @param ackTimeout
     *            the new timeout in milliseconds
     */
    public void setAckTimeout(long ackTimeout) {
        this.ackTimeout = ackTimeout;
    }

    @Override
    public void close() throws IOException {
        try {
            ackChannel.close();
        } catch (TimeoutException e) {
        }
        super.close();
    }
}
