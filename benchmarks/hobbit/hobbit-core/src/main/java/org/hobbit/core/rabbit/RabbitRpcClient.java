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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.IOUtils;
import org.hobbit.core.data.RabbitQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AbstractFuture;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

/**
 * This class implements a thread safe client that can process several RPC calls
 * in parallel.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class RabbitRpcClient implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitRpcClient.class);

    /**
     * The default maximum amount of time in millisecond the client is waiting
     * for a response = {@value #DEFAULT_MAX_WAITING_TIME}ms.
     */
    private static final long DEFAULT_MAX_WAITING_TIME = 600000;

    /**
     * Creates a StorageServiceClient using the given RabbitMQ
     * {@link Connection}.
     *
     * @param connection
     *            RabbitMQ connection used for the communication
     * @param requestQueueName
     *            name of the queue to which the requests should be sent
     * @return a StorageServiceClient instance
     * @throws IOException
     *             if a problem occurs during the creation of the queues or the
     *             consumer.
     */
    public static RabbitRpcClient create(Connection connection, String requestQueueName) throws IOException {
        RabbitRpcClient client = new RabbitRpcClient();
        try {
            client.init(connection, requestQueueName);
            return client;
        } catch (Exception e) {
            client.close();
            throw e;
        }
    }

    /**
     * Constructor.
     */
    protected RabbitRpcClient() {
    }

    /**
     * Queue used for the request.
     */
    private RabbitQueue requestQueue;
    /**
     * Queue used for the responses.
     */
    private RabbitQueue responseQueue;
    /**
     * Mutex for managing access to the {@link #currentRequests} object.
     */
    private Semaphore requestMapMutex = new Semaphore(1);
    /**
     * Mapping of correlation Ids to their {@link RabbitRpcRequest} instances.
     */
    private Map<String, RabbitRpcRequest> currentRequests = new HashMap<String, RabbitRpcRequest>();
    /**
     * The maximum amount of time in millisecond the client is waiting for a
     * response. The default value is defined by
     * {@link #DEFAULT_MAX_WAITING_TIME}.
     */
    private long maxWaitingTime = DEFAULT_MAX_WAITING_TIME;

    /**
     * Initializes the client by declaring a request queue using the given
     * connection and queue name as well as a second queue and a consumer for
     * retrieving responses.
     *
     * @param connection
     *            the RabbitMQ connection that is used for creating queues
     * @param requestQueueName
     *            the name of the queue
     * @throws IOException
     *             if a communication problem during the creation of the
     *             channel, the queue or the internal consumer occurs
     */
    protected void init(Connection connection, String requestQueueName) throws IOException {
        Channel tempChannel = connection.createChannel();
        tempChannel.queueDeclare(requestQueueName, false, false, true, null);
        requestQueue = new RabbitQueue(tempChannel, requestQueueName);
        tempChannel = connection.createChannel();
        responseQueue = new RabbitQueue(tempChannel, tempChannel.queueDeclare().getQueue());
        responseQueue.channel.basicQos(1);
        RabbitRpcClientConsumer consumer = new RabbitRpcClientConsumer(responseQueue.channel, this);
        responseQueue.channel.basicConsume(responseQueue.name, true, consumer);
    }

    /**
     * Sends the request, i.e., the given data, and blocks until the response is
     * received.
     *
     * @param data
     *            the data of the request
     * @return the response or null if an error occurs.
     */
    public byte[] request(byte[] data) {
        byte[] response = null;
        try {
            String corrId = java.util.UUID.randomUUID().toString();

            BasicProperties props = new BasicProperties.Builder().correlationId(corrId).deliveryMode(2)
                    .replyTo(responseQueue.name).build();
            RabbitRpcRequest request = new RabbitRpcRequest();
            requestMapMutex.acquire();
            currentRequests.put(corrId, request);
            requestMapMutex.release();

            requestQueue.channel.basicPublish("", requestQueue.name, props, data);

            response = request.get(maxWaitingTime, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            LOGGER.error("Timeout while sending query. Returning null.");
        } catch (Exception e) {
            LOGGER.error("Exception while sending query. Returning null.", e);
        }
        return response;
    }

    /**
     * Processes the response with the given correlation Id and byte array by
     * searching for a matching request and setting the response if it could be
     * found. If there is no request with the same correlation Id, nothing is
     * done.
     *
     * @param corrId
     *            correlation Id of the response
     * @param body
     *            data of the response
     */
    protected void processResponseForRequest(String corrId, byte[] body) {
        if (currentRequests.containsKey(corrId)) {
            try {
                requestMapMutex.acquire();
                currentRequests.get(corrId).setResponse(body);
                currentRequests.remove(corrId);
                requestMapMutex.release();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public long getMaxWaitingTime() {
        return maxWaitingTime;
    }

    /**
     * Sets the maximum amount of time the client is waiting for a response.
     *
     * @param maxWaitingTime
     *            the maximum waiting time in milliseconds
     */
    public void setMaxWaitingTime(long maxWaitingTime) {
        this.maxWaitingTime = maxWaitingTime;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(requestQueue);
        IOUtils.closeQuietly(responseQueue);
    }

    /**
     * Internal implementation of a Consumer that receives messages on the reply
     * queue and calls
     * {@link RabbitRpcClient#processResponseForRequest(String, byte[])} of its
     * {@link #client}.
     *
     * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
     *
     */
    protected static class RabbitRpcClientConsumer extends DefaultConsumer {

        /**
         * The client for which this instance is acting as consumer.
         */
        private RabbitRpcClient client;

        /**
         * Constructor.
         *
         * @param channel
         *            channel from which the messages are received
         * @param client
         *            the client for which this instance is acting as consumer
         */
        public RabbitRpcClientConsumer(Channel channel, RabbitRpcClient client) {
            super(channel);
            this.client = client;
        }

        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                throws IOException {
            try {
                String corrId = properties.getCorrelationId();
                if (corrId != null) {
                    client.processResponseForRequest(corrId, body);
                }
            } catch (Exception e) {
                LOGGER.error("Exception while processing response.", e);
            }
        }
    }

    /**
     * Simple extension of the {@link AbstractFuture} class that waits for the
     * response which is set by the {@link #setResponse(byte[] response)}.
     *
     * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
     *
     */
    protected static class RabbitRpcRequest extends AbstractFuture<byte[]> implements Future<byte[]> {

        /**
         * Calls the internal set method of the {@link AbstractFuture} class.
         *
         * @param response
         *            the response this request is waiting for
         */
        public void setResponse(byte[] response) {
            set(response);
        }
    }
}
