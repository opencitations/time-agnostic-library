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
package org.hobbit.core.data;

import java.io.Closeable;
import java.io.IOException;

import com.rabbitmq.client.Channel;

/**
 * Simple structure representing the data of a RabbitMQ queue, i.e., the
 * {@link Channel} and the name.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class RabbitQueue implements Closeable {

    /**
     * Channel of this queue.
     */
    public final Channel channel;
    /**
     * Name of this queue.
     */
    public final String name;

    /**
     * Constructor.
     *
     * @param channel
     *            Channel of this queue.
     * @param name
     *            Name of this queue.
     */
    public RabbitQueue(Channel channel, String name) {
        this.channel = channel;
        this.name = name;
    }

    public Channel getChannel() {
        return channel;
    }

    public String getName() {
        return name;
    }

    /**
     * Returns number of available messages or 0 if an error occurred.
     *
     * @return the number of available messages.
     */
    public long messageCount() {
        try {
            return channel.messageCount(name);
        } catch (IOException e) {
            return 0;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("RabbitQueue [channel=");
        builder.append(channel);
        builder.append(", name=");
        builder.append(name);
        builder.append("]");
        return builder.toString();
    }

    @Override
    public void close() throws IOException {
        try {
            channel.close();
        } catch (Exception e) {
        }
    }
}
