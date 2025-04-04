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
import java.util.concurrent.Semaphore;

import org.apache.commons.io.IOUtils;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.rabbit.DataSender;
import org.hobbit.core.rabbit.DataSenderImpl;

public abstract class AbstractDataGenerator extends AbstractPlatformConnectorComponent {

    private Semaphore startDataGenMutex = new Semaphore(0);
    private int generatorId;
    private int numberOfGenerators;
    protected DataSender sender2TaskGen;
    protected DataSender sender2System;

    public AbstractDataGenerator() {
        defaultContainerType = Constants.CONTAINER_TYPE_BENCHMARK;
    }

    @Override
    public void init() throws Exception {
        super.init();

        generatorId = configuration.getInt(Constants.GENERATOR_ID_KEY);
        numberOfGenerators = configuration.getInt(Constants.GENERATOR_COUNT_KEY);

        sender2TaskGen = DataSenderImpl.builder().queue(getFactoryForOutgoingDataQueues(),
                generateSessionQueueName(Constants.DATA_GEN_2_TASK_GEN_QUEUE_NAME)).build();
        sender2System = DataSenderImpl.builder().queue(getFactoryForOutgoingDataQueues(),
                generateSessionQueueName(Constants.DATA_GEN_2_SYSTEM_QUEUE_NAME)).build();
    }

    @Override
    public void run() throws Exception {
        try {
            sendToCmdQueue(Commands.DATA_GENERATOR_READY_SIGNAL);
            // Wait for the start message
            startDataGenMutex.acquire();

            generateData();

            // We have to wait until all messages are consumed
            sender2TaskGen.closeWhenFinished();
            sender2System.closeWhenFinished();
        } catch (Exception e) {
            throw reportAndWrap(e);
        }
    }

    protected abstract void generateData() throws Exception;

    @Override
    public void receiveCommand(byte command, byte[] data) {
        // If this is the signal to start the data generation
        if (command == Commands.DATA_GENERATOR_START_SIGNAL) {
            // release the mutex
            startDataGenMutex.release();
        }
        super.receiveCommand(command, data);
    }

    protected void sendDataToTaskGenerator(byte[] data) throws IOException {
        sender2TaskGen.sendData(data);
    }

    protected void sendDataToSystemAdapter(byte[] data) throws IOException {
        sender2System.sendData(data);
    }

    public int getGeneratorId() {
        return generatorId;
    }

    public int getNumberOfGenerators() {
        return numberOfGenerators;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(sender2TaskGen);
        IOUtils.closeQuietly(sender2System);
        super.close();
    }
}
