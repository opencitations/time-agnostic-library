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

import java.io.IOException;

import org.hobbit.core.data.RabbitQueue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class RabbitQueueFactoryImpl implements RabbitQueueFactory {

    private final Connection connection;

    public RabbitQueueFactoryImpl(Connection connection) {
        this.connection = connection;
    }

    @Override
    public RabbitQueue createDefaultRabbitQueue(String name) throws IOException {
        return createDefaultRabbitQueue(name, createChannel());
    }

    public Connection getConnection() {
        return connection;
    }

    public void close() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
            }
        }
    }

    @Override
    public RabbitQueue createDefaultRabbitQueue(String name, Channel channel) throws IOException {
        channel.queueDeclare(name, false, false, true, null);
        return new RabbitQueue(channel, name);
    }

    @Override
    public Channel createChannel() throws IOException {
        return connection.createChannel();
    }
}
