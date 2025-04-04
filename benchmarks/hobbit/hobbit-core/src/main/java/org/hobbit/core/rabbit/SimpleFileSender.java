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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.io.IOUtils;
import org.hobbit.core.data.RabbitQueue;

import com.rabbitmq.client.MessageProperties;

/**
 * A simple class that can be used to send files, i.e., small or large sized
 * data with a single name, via RabbitMQ using a queue with the given name. A
 * message created by this implementation comprises the following data
 * <ol>
 * <li>int length of file name</li>
 * <li>byte[] file name</li>
 * <li>int message id</li>
 * <li>byte[] data</li>
 * </ol>
 * The end of the file is indicated by a message with an empty data array.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class SimpleFileSender implements Closeable {

    private static final int DEFAULT_MESSAGE_SIZE = 65536;

    public static SimpleFileSender create(RabbitQueueFactory factory, String queueName) throws IOException {
        return new SimpleFileSender(factory.createDefaultRabbitQueue(queueName));
    }

    private RabbitQueue queue;
    private int messageSize = DEFAULT_MESSAGE_SIZE;

    protected SimpleFileSender(RabbitQueue queue) {
        this.queue = queue;
    }

    public void streamData(InputStream is, String name) throws IOException {
        int messageId = 0;
        int length = 0;
        byte[] nameBytes = RabbitMQUtils.writeString(name);
        byte[] array = new byte[messageSize + nameBytes.length + 8];
        ByteBuffer buffer = ByteBuffer.wrap(array);
        buffer.putInt(nameBytes.length);
        buffer.put(nameBytes);
        int messageIdPos = buffer.position();
        int dataStartPos = messageIdPos + 4;
        do {
            buffer.position(messageIdPos);
            buffer.putInt(messageId);
            length = is.read(array, dataStartPos, array.length - dataStartPos);
            queue.channel.basicPublish("", queue.name, MessageProperties.MINIMAL_PERSISTENT_BASIC,
                    Arrays.copyOf(array, (length > 0) ? (dataStartPos + length) : dataStartPos));
            ++messageId;
        } while (length > 0);
    }

    public void setMessageSize(int messageSize) {
        this.messageSize = messageSize;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(queue);
    }

}
