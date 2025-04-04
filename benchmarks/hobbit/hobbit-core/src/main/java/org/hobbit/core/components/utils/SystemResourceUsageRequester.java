package org.hobbit.core.components.utils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.Charsets;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractCommandReceivingComponent;
import org.hobbit.core.components.PlatformConnector;
import org.hobbit.core.data.usage.ResourceUsageInformation;
import org.hobbit.core.rabbit.QueueingConsumer;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Delivery;

public class SystemResourceUsageRequester implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemResourceUsageRequester.class);

    /**
     * Consumer of the queue that is used to receive responses for messages that are
     * sent via the command queue and for which an answer is expected.
     */
    private QueueingConsumer responseConsumer = null;
    
    public static SystemResourceUsageRequester create(PlatformConnector connector, String sessionId) {
        try {
            Channel cmdChannel = connector.getFactoryForOutgoingCmdQueues().createChannel();
            Channel incomingChannel = connector.getFactoryForIncomingDataQueues().createChannel();
            String responseQueueName = null;
            // if (responseQueueName == null) {
            responseQueueName = incomingChannel.queueDeclare().getQueue();
            // }
            QueueingConsumer responseConsumer = new QueueingConsumer(cmdChannel);
        	incomingChannel.basicConsume(responseQueueName, responseConsumer);
            return new SystemResourceUsageRequester(cmdChannel, incomingChannel, responseQueueName, responseConsumer, sessionId);
        } catch (Exception e) {
            LOGGER.error("Exception while creating SystemResourceUsageRequester. Returning null.", e);
        }
        return null;
    }

    /**
     * Name of the queue that is used to receive responses for messages that are
     * sent via the command queue and for which an answer is expected.
     */
    private String responseQueueName = null;
    
    /**
     * Channel that is used for the command queue but not owned by this class (i.e.,
     * it won't be closed).
     */
    protected Channel cmdChannel = null;

    protected Channel incomingChannel = null;

    protected byte sessionIdBytes[];

    protected Gson gson = new Gson();

    protected SystemResourceUsageRequester(Channel cmdChannel, Channel incomingChannel, String responseQueueName,
    		QueueingConsumer responseConsumer, String sessionId) {
    	
        this.cmdChannel = cmdChannel;
        this.incomingChannel = incomingChannel;
        this.responseQueueName = responseQueueName;
        this.responseConsumer = responseConsumer;
        this.sessionIdBytes = sessionId.getBytes(Charsets.UTF_8);
    }

    public ResourceUsageInformation getSystemResourceUsage() {
        try {
            BasicProperties props = new BasicProperties.Builder().deliveryMode(2).replyTo(responseQueueName).build();
            sendToCmdQueue(Commands.REQUEST_SYSTEM_RESOURCES_USAGE, null, props);
            Delivery delivery = responseConsumer.getDeliveryQueue().poll(AbstractCommandReceivingComponent.DEFAULT_CMD_RESPONSE_TIMEOUT, 
            		TimeUnit.MILLISECONDS);
            Objects.requireNonNull(delivery, "Didn't got a response for a create container message.");
            if (delivery.getBody().length > 0) {
                return gson.fromJson(RabbitMQUtils.readString(delivery.getBody()), ResourceUsageInformation.class);
            }
        } catch (Exception e) {
            LOGGER.error("Got exception while trying to request the system resource usage statistics.", e);
        }
        return null;
    }

    /**
     * Sends the given command to the command queue with the given data appended and
     * using the given properties.
     *
     * @param command
     *            the command that should be sent
     * @param data
     *            data that should be appended to the command
     * @param props
     *            properties that should be used for the message
     * @throws IOException
     *             if a communication problem occurs
     */
    protected void sendToCmdQueue(byte command, byte data[], BasicProperties props) throws IOException {
        // + 5 because 4 bytes for the session ID length and 1 byte for the
        // command
        int dataLength = sessionIdBytes.length + 5;
        boolean attachData = (data != null) && (data.length > 0);
        if (attachData) {
            dataLength += data.length;
        }
        ByteBuffer buffer = ByteBuffer.allocate(dataLength);
        buffer.putInt(sessionIdBytes.length);
        buffer.put(sessionIdBytes);
        buffer.put(command);
        if (attachData) {
            buffer.put(data);
        }
        cmdChannel.basicPublish(Constants.HOBBIT_COMMAND_EXCHANGE_NAME, "", props, buffer.array());
    }

    @Override
    public void close() throws IOException {
        if (cmdChannel != null) {
            try {
                cmdChannel.close();
            } catch (Exception e) {
            }
        }
        if (incomingChannel != null) {
            try {
                incomingChannel.close();
            } catch (Exception e) {
            }
        }
    }
}
