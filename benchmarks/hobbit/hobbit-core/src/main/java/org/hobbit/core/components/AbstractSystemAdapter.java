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
import java.util.concurrent.Semaphore;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.rabbit.DataHandler;
import org.hobbit.core.rabbit.DataReceiver;
import org.hobbit.core.rabbit.DataReceiverImpl;
import org.hobbit.core.rabbit.DataSender;
import org.hobbit.core.rabbit.DataSenderImpl;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class implements basic functions that can be used to implement
 * a system adapter.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public abstract class AbstractSystemAdapter extends AbstractPlatformConnectorComponent
        implements GeneratedDataReceivingComponent, TaskReceivingComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSystemAdapter.class);

    /**
     * Default value of the {@link #maxParallelProcessedMsgs} attribute.
     */
    private static final int DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES = 100;

    /**
     * Mutex used to wait for the terminate signal.
     */
    private Semaphore terminateMutex = new Semaphore(0);
    /**
     * The cause for an unusual termination.
     */
    private Exception cause;
    /**
     * Mutex used to manage access to the {@link #cause} object.
     */
    private Semaphore causeMutex = new Semaphore(1);
    /**
     * The maximum number of incoming messages of a single queue that are processed
     * in parallel. Additional messages have to wait.
     */
    private final int maxParallelProcessedMsgs;
    /**
     * Receiver for data coming from the data generator.
     */
    protected DataReceiver dataGenReceiver;
    /**
     * Receiver for tasks coming from the task generator.
     */
    protected DataReceiver taskGenReceiver;
    /**
     * Sender for sending messages from the benchmarked system to the evaluation
     * storage.
     */
    protected DataSender sender2EvalStore;
    /**
     * The RDF model containing the system parameters.
     */
    protected Model systemParamModel;

    /**
     * Constructor using the {@link #DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES}=
     * {@value #DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES}.
     */
    public AbstractSystemAdapter() {
        this(DEFAULT_MAX_PARALLEL_PROCESSED_MESSAGES);
    }

    /**
     * Constructor setting the maximum number of messages processed in parallel.
     *
     * @param maxParallelProcessedMsgs The maximum number of incoming messages of a
     *                                 single queue that are processed in parallel.
     *                                 Additional messages have to wait.
     */
    public AbstractSystemAdapter(int maxParallelProcessedMsgs) {
        this.maxParallelProcessedMsgs = maxParallelProcessedMsgs;
        defaultContainerType = Constants.CONTAINER_TYPE_SYSTEM;
    }

    @Override
    public void init() throws Exception {
        super.init();

        // Get the benchmark parameter model
        systemParamModel = configuration.getModel(Constants.SYSTEM_PARAMETERS_MODEL_KEY,
                () -> ModelFactory.createDefaultModel(), LOGGER);

        dataGenReceiver = DataReceiverImpl.builder().maxParallelProcessedMsgs(maxParallelProcessedMsgs)
                .queue(incomingDataQueueFactory, generateSessionQueueName(Constants.DATA_GEN_2_SYSTEM_QUEUE_NAME))
                .dataHandler(new DataHandler() {
                    @Override
                    public void handleData(byte[] data) {
                        receiveGeneratedData(data);
                    }
                }).build();

        taskGenReceiver = DataReceiverImpl.builder().maxParallelProcessedMsgs(maxParallelProcessedMsgs)
                .queue(incomingDataQueueFactory, generateSessionQueueName(Constants.TASK_GEN_2_SYSTEM_QUEUE_NAME))
                .dataHandler(new DataHandler() {
                    @Override
                    public void handleData(byte[] data) {
                        ByteBuffer buffer = ByteBuffer.wrap(data);
                        String taskId = RabbitMQUtils.readString(buffer);
                        byte[] taskData = RabbitMQUtils.readByteArray(buffer);
                        receiveGeneratedTask(taskId, taskData);
                    }
                }).build();

        sender2EvalStore = DataSenderImpl.builder().queue(getFactoryForOutgoingDataQueues(),
                generateSessionQueueName(Constants.SYSTEM_2_EVAL_STORAGE_DEFAULT_QUEUE_NAME)).build();
    }

    @Override
    public void run() throws Exception {
        try {
            sendToCmdQueue(Commands.SYSTEM_READY_SIGNAL);

            terminateMutex.acquire();
            // Check whether the system should abort
            try {
                causeMutex.acquire();
                if (cause != null) {
                    throw cause;
                }
                causeMutex.release();
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted while waiting to set the termination cause.");
            }
            // Close receivers as soon as all messages have been received
            dataGenReceiver.closeWhenFinished();
            taskGenReceiver.closeWhenFinished();
        } catch (Exception e) {
            throw reportAndWrap(e);
        }
    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        // If this is the signal to start the data generation
        if (command == Commands.TASK_GENERATION_FINISHED) {
            terminate(null);
        }
        super.receiveCommand(command, data);
    }

    /**
     * This method sends the given result data for the task with the given task id
     * to the evaluation storage.
     *
     * @param taskIdString the id of the task
     * @param data         the data of the task
     * @throws IOException if there is an error during the sending
     */
    protected void sendResultToEvalStorage(String taskIdString, byte[] data) throws IOException {
        byte[] taskIdBytes = taskIdString.getBytes(Charsets.UTF_8);
        // + 4 for taskIdBytes.length
        // + 4 for data.length
        int capacity = 4 + 4 + taskIdBytes.length + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(capacity);
        buffer.putInt(taskIdBytes.length);
        buffer.put(taskIdBytes);
        buffer.putInt(data.length);
        buffer.put(data);
        sender2EvalStore.sendData(buffer.array());
    }

    /**
     * Starts termination of the main thread of this system adapter. If a cause is
     * given, it will be thrown causing an abortion from the main thread instead of
     * a normal termination.
     *
     * @param cause the cause for an abortion of the process or {code null} if the
     *              component should terminate in a normal way.
     */
    protected synchronized void terminate(Exception cause) {
        if (cause != null) {
            try {
                causeMutex.acquire();
                this.cause = cause;
                causeMutex.release();
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted while waiting to set the termination cause.");
            }
        }
        terminateMutex.release();
    }

    @Override
    public void close() throws IOException {
        // Make sure that the receivers are closed
        IOUtils.closeQuietly(dataGenReceiver);
        IOUtils.closeQuietly(taskGenReceiver);
        // Close the sender (we shouldn't close it before this point since we want to be
        // sure that all results have been sent)
        sender2EvalStore.closeWhenFinished();
        super.close();
    }
}
