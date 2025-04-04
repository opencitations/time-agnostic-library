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

import org.hobbit.core.data.RabbitQueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public interface RabbitQueueFactory extends Closeable {

    /**
     * This method opens a channel using the established connection to RabbitMQ
     * and creates a new queue using the given name and the following
     * configuration:
     * <ul>
     * <li>The channel number is automatically derived from the connection.</li>
     * <li>The queue is not durable.</li>
     * <li>The queue is not exclusive.</li>
     * <li>The queue is configured to be deleted automatically.</li>
     * <li>No additional queue configuration is defined.</li>
     * </ul>
     *
     * @param name
     *            name of the queue
     * @return {@link RabbitQueue} object comprising the {@link Channel} and the
     *         name of the created queue
     * @throws IOException
     *             if a communication problem during the creation of the channel
     *             or the queue occurs
     */
    public RabbitQueue createDefaultRabbitQueue(String name) throws IOException;

    /**
     * This method uses the given channel and creates a new queue using the
     * given name and the following configuration:
     * <ul>
     * <li>The queue is not durable.</li>
     * <li>The queue is not exclusive.</li>
     * <li>The queue is configured to be deleted automatically.</li>
     * <li>No additional queue configuration is defined.</li>
     * </ul>
     *
     * @param name
     *            name of the queue
     * @param channel
     *            the {@link Channel} that will be used to generate the queue
     * @return {@link RabbitQueue} object comprising the {@link Channel} and the
     *         name of the created queue
     * @throws IOException
     *             if a communication problem during the creation of the channel
     *             or the queue occurs
     */
    public RabbitQueue createDefaultRabbitQueue(String name, Channel channel) throws IOException;

    /**
     * This method opens a channel using the established connection to RabbitMQ.
     * The channel number is automatically derived from the connection.
     *
     * @return the newly created {@link Channel}
     * @throws IOException
     *             if a communication problem during the creation of the channel
     */
    public Channel createChannel() throws IOException;

    /**
     * Returns the {@link Connection} to the RabbitMQ broker used internally.
     * 
     * @return the {@link Connection} to the RabbitMQ broker used internally
     */
    public Connection getConnection();
}
