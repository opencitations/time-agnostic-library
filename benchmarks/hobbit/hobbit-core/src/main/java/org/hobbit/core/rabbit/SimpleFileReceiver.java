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
package org.hobbit.core.rabbit;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.hobbit.core.data.FileReceiveState;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.ShutdownSignalException;

/**
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class SimpleFileReceiver {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleFileReceiver.class);

    protected static final long DEFAULT_TIMEOUT = 1000;

    protected QueueingConsumer consumer;
    
    public static SimpleFileReceiver create(RabbitQueueFactory factory, String queueName) throws IOException {
        return create(factory.createDefaultRabbitQueue(queueName));
    }

    public static SimpleFileReceiver create(RabbitQueue queue) throws IOException {
        QueueingConsumer consumer = new QueueingConsumer(queue.channel);
        queue.channel.basicConsume(queue.name, true, consumer);
        queue.channel.basicQos(20);
        return new SimpleFileReceiver(queue, consumer);
    }

    protected RabbitQueue queue;
    
    protected Map<String, FileReceiveState> fileStates = new HashMap<>();
    protected boolean terminated = false;
    protected int errorCount = 0;
    protected ExecutorService executor = Executors.newCachedThreadPool();
    protected long waitingForMsgTimeout = DEFAULT_TIMEOUT;

    protected SimpleFileReceiver(RabbitQueue queue, QueueingConsumer consumer) {
        this.queue = queue;
        this.consumer = consumer;
    }

    public String[] receiveData(String outputDirectory)
            throws IOException, ShutdownSignalException, ConsumerCancelledException, InterruptedException {
        if (!outputDirectory.endsWith(File.separator)) {
            outputDirectory = outputDirectory + File.separator;
        }
        File outDir = new File(outputDirectory);
        // Create output directory if it does not exist
        if (!outDir.exists() && !outDir.mkdirs()) {
                throw new IOException("Couldn't create \"" + outDir.getAbsolutePath() + "\".");
        }
        try {
            Delivery delivery = null;
            // while the receiver should not terminate, the last delivery was
            // not empty or there are still deliveries in the (servers) queue
            while ((!terminated) || (delivery != null) || (queue.channel.messageCount(queue.name) > 0)) {
                delivery = consumer.getDeliveryQueue().poll(waitingForMsgTimeout, TimeUnit.MILLISECONDS);
                if (delivery != null) {
                    executor.execute(new MessageProcessing(this, outputDirectory, delivery.getBody()));
                }
            }
        } finally {
            close();
        }
        return fileStates.keySet().toArray(new String[fileStates.size()]);
    }

    public void terminate() {
        terminated = true;
    }

    public void forceTermination() {
        terminated = true;
        close();
    }

    protected synchronized void increaseErrorCount() {
        ++errorCount;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public void setWaitingForMsgTimeout(long waitingForMsgTimeout) {
        this.waitingForMsgTimeout = waitingForMsgTimeout;
    }

    protected void close() {
        executor.shutdown();
        // We will wait up to 10 seconds if one of the tasks is still
        // running
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for executor to terminate.");
        }
        IOUtils.closeQuietly(queue);
        for (String fileName : fileStates.keySet()) {
            if (fileStates.get(fileName).outputStream != null) {
                LOGGER.warn("Closing file \"{}\" for which no end message has been received.", fileName);
                IOUtils.closeQuietly(fileStates.get(fileName).outputStream);
                increaseErrorCount();
            }
        }
    }

    protected static class MessageProcessing implements Runnable {

        private SimpleFileReceiver receiver;
        private String outputDir;
        private byte[] data;

        public MessageProcessing(SimpleFileReceiver receiver, String outputDir, byte[] data) {
            this.outputDir = outputDir;
            this.data = data;
            this.receiver = receiver;
        }

        @Override
        public void run() {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            String filename = RabbitMQUtils.readString(buffer);
            FileReceiveState state = null;
            synchronized (receiver.fileStates) {
                if (receiver.fileStates.containsKey(filename)) {
                    state = receiver.fileStates.get(filename);
                } else {
                    try {
                        state = new FileReceiveState(filename,
                                new BufferedOutputStream(new FileOutputStream(outputDir + filename)));
                    } catch (FileNotFoundException e) {
                        LOGGER.error("Couldn't create file \"" + filename + "\". Message will be ignored.", e);
                        receiver.increaseErrorCount();
                        return;
                    }
                    receiver.fileStates.put(filename, state);
                }
            }
            synchronized (state) {
                // read the id of this message
                int messageId = buffer.getInt();
                byte[] messageData = ArrayUtils.subarray(buffer.array(), buffer.position(), buffer.array().length);
                if (messageId == state.nextMessageId) {
                    processMessageData(messageData, state);
                } else {
                    state.messageBuffer.put(messageId, messageData);
                }
            }
        }

        protected void processMessageData(byte[] messageData, FileReceiveState state) {
            // if this is the last message for this file
            if (messageData.length == 0) {
                // try {
                // state.outputStream.flush();
                // } catch (IOException e) {
                // LOGGER.error("Exception while trying to flush data.", e);
                // }
                IOUtils.closeQuietly(state.outputStream);
                state.outputStream = null;
                LOGGER.debug("Received last message for file \"{}\".", state.name);
                if (state.messageBuffer.size() > 0) {
                    LOGGER.error("Closed the file \"{}\" while there are still {} messages in its data buffer",
                            state.name, state.messageBuffer.size());
                }
            } else {
                // write the data to file
                try {
                    state.outputStream.write(messageData);
                } catch (IOException e) {
                    LOGGER.error("Couldn't write message data to file.", e);
                    receiver.increaseErrorCount();
                }
                ++state.nextMessageId;
                // If there is a message in the buffer that should be written
                // now
                if (state.messageBuffer.containsKey(state.nextMessageId)) {
                    messageData = state.messageBuffer.remove(state.nextMessageId);
                    processMessageData(messageData, state);
                }
            }
        }
    }

}
