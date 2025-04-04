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

import org.apache.commons.configuration2.EnvironmentConfiguration;
import org.apache.commons.io.IOUtils;
import org.hobbit.core.Constants;
import org.hobbit.core.rabbit.RabbitQueueFactory;
import org.hobbit.core.rabbit.RabbitQueueFactoryImpl;
import org.hobbit.utils.config.HobbitConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * This abstract class implements basic functionalities of a hobbit component.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public abstract class AbstractComponent implements Component {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractComponent.class);

    /**
     * Maximum number of retries that are executed to connect to RabbitMQ.
     */
    public static final int NUMBER_OF_RETRIES_TO_CONNECT_TO_RABBIT_MQ = 5;
    /**
     * Time, the system waits before retrying to connect to RabbitMQ. Note that this
     * time will be multiplied with the number of already failed tries.
     */
    public static final long START_WAITING_TIME_BEFORE_RETRY = 5000;

    private String hobbitSessionId;
    /**
     * Factory for creating outgoing data queues.
     */
    protected RabbitQueueFactory outgoingDataQueuefactory = null;
    /**
     * Factory for creating outgoing data queues.
     */
    protected RabbitQueueFactory incomingDataQueueFactory = null;
    /**
     * The host name of the RabbitMQ broker.
     */
    protected String rabbitMQHostName;
    /**
     * The factory that can be used to create additional connections. However, in
     * most cases it is sufficient to create a new channel using the already
     * existing {@link #incomingDataQueueFactory} and
     * {@link #outgoingDataQueuefactory} objects.
     */
    protected ConnectionFactory connectionFactory;
    /**
     * Configuration that is used by this component.
     */
    protected HobbitConfiguration configuration;

    /**
     * Default constructor. It will create a configuration object that is solely
     * relying on environment variables. This is the same as using the following
     * lines of code:
     * 
     * <pre>
     * HobbitConfiguration configuration = new HobbitConfiguration();
     * configuration.addConfiguration(new EnvironmentConfiguration());
     * AbstractComponent component = new AbstractComponent();
     * component.setConfiguration(configuration);
     * </pre>
     */
    public AbstractComponent() {
        configuration = new HobbitConfiguration();
        configuration.addConfiguration(new EnvironmentConfiguration());
    }

    @Override
    public void init() throws Exception {
        hobbitSessionId = configuration.getString(Constants.HOBBIT_SESSION_ID_KEY,
                Constants.HOBBIT_SESSION_ID_FOR_PLATFORM_COMPONENTS);
        if (rabbitMQHostName == null) {
            rabbitMQHostName = configuration.getString(Constants.RABBIT_MQ_HOST_NAME_KEY, LOGGER);
        }
        connectionFactory = new ConnectionFactory();
        if (rabbitMQHostName.contains(":")) {
            String[] splitted = rabbitMQHostName.split(":");
            connectionFactory.setHost(splitted[0]);
            connectionFactory.setPort(Integer.parseInt(splitted[1]));
        } else
            connectionFactory.setHost(rabbitMQHostName);
        connectionFactory.setAutomaticRecoveryEnabled(true);
        // attempt recovery every 10 seconds
        connectionFactory.setNetworkRecoveryInterval(10000);
        incomingDataQueueFactory = new RabbitQueueFactoryImpl(createConnection());
        outgoingDataQueuefactory = new RabbitQueueFactoryImpl(createConnection());
    }

    protected Connection createConnection() throws IOException {
        Connection connection = null;
        Exception exception = null;
        for (int i = 0; (connection == null) && (i <= NUMBER_OF_RETRIES_TO_CONNECT_TO_RABBIT_MQ); ++i) {
            try {
                connection = connectionFactory.newConnection();
            } catch (Exception e) {
                if (i < NUMBER_OF_RETRIES_TO_CONNECT_TO_RABBIT_MQ) {
                    long waitingTime = START_WAITING_TIME_BEFORE_RETRY * (i + 1);
                    LOGGER.warn("Couldn't connect to RabbitMQ with try #" + i + ". Next try in " + waitingTime + "ms.");
                    exception = e;
                    try {
                        Thread.sleep(waitingTime);
                    } catch (Exception e2) {
                        LOGGER.warn("Interrupted while waiting before retrying to connect to RabbitMQ.", e2);
                    }
                }
            }
        }
        if (connection == null) {
            String msg = "Couldn't connect to RabbitMQ after " + NUMBER_OF_RETRIES_TO_CONNECT_TO_RABBIT_MQ
                    + " retries.";
            LOGGER.error(msg, exception);
            throw new IOException(msg, exception);
        }
        return connection;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(incomingDataQueueFactory);
        IOUtils.closeQuietly(outgoingDataQueuefactory);
    }

    public String getHobbitSessionId() {
        return hobbitSessionId;
    }

    public String generateSessionQueueName(String queueName) {
        return queueName + "." + hobbitSessionId;
    }

    /**
     * @return the configuration
     */
    public HobbitConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * @param configuration the configuration that is used by this component
     */
    @Override
    public void setConfiguration(HobbitConfiguration configuration) {
        this.configuration = configuration;
    }

}
