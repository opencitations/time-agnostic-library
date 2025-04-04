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
package org.hobbit.core.components.dummy;

import org.hobbit.core.Constants;
import org.hobbit.core.rabbit.RabbitMQUtils;
import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import org.apache.commons.io.Charsets;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import org.hobbit.core.components.AbstractCommandReceivingComponent;

import org.junit.Ignore;

@Ignore
public abstract class AbstractDummyPlatformController extends AbstractCommandReceivingComponent {
    private boolean readyFlag = false;
    private Semaphore terminationMutex = new Semaphore(0);

    public AbstractDummyPlatformController() {
        super();
    }

    public AbstractDummyPlatformController(boolean execCommandsInParallel) {
        super(execCommandsInParallel);
    }

    @Override
    public void run() throws Exception {
        readyFlag = true;
        terminationMutex.acquire();
    }

    public void waitForControllerBeingReady() throws InterruptedException {
        while (!readyFlag) {
            Thread.sleep(500);
        }
    }

    @Override
    protected void handleCmd(byte bytes[], AMQP.BasicProperties props) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int idLength = buffer.getInt();
        byte sessionIdBytes[] = new byte[idLength];
        buffer.get(sessionIdBytes);
        String sessionId = new String(sessionIdBytes, Charsets.UTF_8);
        byte command = buffer.get();
        byte remainingData[];
        if (buffer.remaining() > 0) {
            remainingData = new byte[buffer.remaining()];
            buffer.get(remainingData);
        } else {
            remainingData = new byte[0];
        }
        receiveCommand(command, remainingData, sessionId, props);
    }

    public void sendToCmdQueue(String address, byte command, byte data[], BasicProperties props)
            throws IOException {
        byte sessionIdBytes[] = RabbitMQUtils.writeString(address);
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
    public void receiveCommand(byte command, byte[] data) {
    }

    public abstract void receiveCommand(byte command, byte[] data, String sessionId, AMQP.BasicProperties props);

    public void terminate() {
        terminationMutex.release();
    }
}
