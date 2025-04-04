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

import org.hobbit.core.Commands;
import org.hobbit.core.rabbit.RabbitQueueFactory;

/**
 * This interface should be implemented by components if they want to offer
 * functionality of the platform to other classes.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public interface PlatformConnector {

    /**
     * This method sends a {@link Commands#DOCKER_CONTAINER_START} command to
     * create and start an instance of the given image using the given
     * environment variables.
     *
     * @param imageName
     *            the name of the image of the docker container
     * @param envVariables
     *            environment variables that should be added to the created
     *            container
     * @param observer
     *            the container state observer that is called if the container
     *            terminates
     * @return the name of the container instance or null if an error occurred
     */
    public String createContainer(String imageName, String[] envVariables, ContainerStateObserver observer);

    /**
     * This method sends a {@link Commands#DOCKER_CONTAINER_STOP} command to
     * stop the container with the given id.
     *
     * @param containerName
     *            the name of the container instance that should be stopped
     */
    public void stopContainer(String containerName);

    public RabbitQueueFactory getFactoryForOutgoingDataQueues();
    public RabbitQueueFactory getFactoryForIncomingDataQueues();
    public RabbitQueueFactory getFactoryForOutgoingCmdQueues();
    public RabbitQueueFactory getFactoryForIncomingCmdQueues();
}
