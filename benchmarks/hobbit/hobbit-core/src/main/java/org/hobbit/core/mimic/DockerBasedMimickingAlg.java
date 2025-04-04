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
package org.hobbit.core.mimic;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.hobbit.core.Constants;
import org.hobbit.core.components.ContainerStateObserver;
import org.hobbit.core.components.PlatformConnector;
import org.hobbit.core.data.RabbitQueue;
import org.hobbit.core.rabbit.SimpleFileReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of a {@link MimickingAlgorithmManager} creates a Docker
 * container for executing the mimicking algorithm and assumes that the
 * algorithm will send its data via a RabbitMQ queue. The name of the queue is
 * given as environment variable with the key
 * {@link Constants#DATA_QUEUE_NAME_KEY}. The generated data is written to a
 * file with the given file name.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class DockerBasedMimickingAlg implements MimickingAlgorithmManager, ContainerStateObserver {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerBasedMimickingAlg.class);

    /**
     * The name of the image containing the mimicking algorithm.
     */
    private String dockerImage;
    /**
     * The connector that offers the needed functionality of the Hobbit
     * platform.
     */
    private PlatformConnector connector;
    private Map<String, SimpleFileReceiver> receivers = new HashMap<>();

    public DockerBasedMimickingAlg(PlatformConnector connector, String dockerImage) {
        this.dockerImage = dockerImage;
        this.connector = connector;
    }
    
    

    @Override
    public void generateData(String outputDirectory, String[] envVariables) throws Exception {
        RabbitQueue queue = null;
        String containerName = null;
        SimpleFileReceiver receiver = null;
        try {
            // create the queue to get data from the container
            queue = connector.getFactoryForIncomingDataQueues()
                    .createDefaultRabbitQueue(UUID.randomUUID().toString().replace("-", ""));
            // create a receiver that writes incoming data to the files
            receiver = SimpleFileReceiver.create(queue);

            // Add the queue name to the environment variables of the container
            envVariables = Arrays.copyOf(envVariables, envVariables.length + 1);
            envVariables[envVariables.length - 1] = Constants.DATA_QUEUE_NAME_KEY + "=" + queue.name;
            // create the container
            containerName = connector.createContainer(dockerImage, envVariables, this);
            if (containerName == null) {
                throw new IOException("Couldn't create container with image \"" + dockerImage + "\".");
            }
            try {
                // Add the created container to the internal mapping
                synchronized (receivers) {
                    // if the key is already there, we have to directly shutdown
                    // the receiver
                    if (receivers.containsKey(containerName)) {
                        receiver.terminate();
                    } else {
                        // add the receiver
                        receivers.put(containerName, receiver);
                    }
                }
                // Receive the data and write it to the files
                receiver.receiveData(outputDirectory);
                // Check whether error occured
                if (receiver.getErrorCount() > 0) {
                    throw new IOException(
                            receiver.getErrorCount() + " errors occured during the receiving of created files.");
                }
            } finally {
                // a problem occurred -> destroy the container
                connector.stopContainer(containerName);
            }
        } finally {
            // close the queue and the file
            IOUtils.closeQuietly(queue);
            if (receiver != null) {
                receiver.forceTermination();
            }
        }
    }

    @Override
    public void containerStopped(String containerName, int exitCode) {
        synchronized (receivers) {
            if (receivers.containsKey(containerName)) {
                receivers.get(containerName).terminate();
                ;
            } else {
                LOGGER.warn("Got a termination message for an unknown container. Adding it.");
                receivers.put(containerName, null);
            }
        }
    }

}
