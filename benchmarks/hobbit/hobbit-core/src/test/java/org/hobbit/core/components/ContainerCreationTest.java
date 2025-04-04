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

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.hobbit.core.components.dummy.DummyComponentExecutor;

import com.rabbitmq.client.AMQP;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.utils.config.HobbitConfiguration;

import java.io.IOException;
import org.hobbit.core.Commands;
import org.hobbit.core.components.dummy.AbstractDummyPlatformController;
import org.hobbit.core.data.StartCommandData;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.hobbit.core.components.dummy.DummyCommandReceivingComponent;
import org.hobbit.core.Constants;
import org.hobbit.core.TestConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.Stopwatch;
import static org.junit.Assert.*;


public class ContainerCreationTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerCreationTest.class);

    private DummyPlatformController platformController = null;

    private static final String HOBBIT_SESSION_ID = "123";
    private static final long CONTAINER_CREATION_DELAY = 2000;

    private AbstractCommandReceivingComponent component;

    @Rule
    public Stopwatch stopwatch = new Stopwatch() {};

    @Before
    public void setUp() throws Exception {
        Configuration configurationVar = new PropertiesConfiguration();
        configurationVar.addProperty(Constants.RABBIT_MQ_HOST_NAME_KEY, TestConstants.RABBIT_HOST);
        configurationVar.addProperty(Constants.HOBBIT_SESSION_ID_KEY, "0");

        HobbitConfiguration configVar = new HobbitConfiguration();
        configVar.addConfiguration(configurationVar);

        platformController = new DummyPlatformController(HOBBIT_SESSION_ID, configVar);
        DummyComponentExecutor platformExecutor = new DummyComponentExecutor(platformController);
        Thread platformThread = new Thread(platformExecutor);
        platformThread.start();
        platformController.waitForControllerBeingReady();

        component = new DummyCommandReceivingComponent(configVar);
        component.init();
    }

    @After
    public void tearDown() throws Exception {
        component.close();
        platformController.terminate();
    }

    @Test(timeout = (long)(CONTAINER_CREATION_DELAY * 2.5))
    public void testSync() throws Exception {
        String containerId1 = component.createContainer("hello-world", null, new String[]{"ID=1", "DELAY="+CONTAINER_CREATION_DELAY});
        String containerId2 = component.createContainer("hello-world", null, new String[]{"ID=2", "DELAY="+CONTAINER_CREATION_DELAY});
        assertEquals("ID of synchronously created container", "1", containerId1);
        assertEquals("ID of synchronously created container", "2", containerId2);
        assertTrue("Spent at least linearly scaled delay while creating containers",
                stopwatch.runtime(TimeUnit.MILLISECONDS) > CONTAINER_CREATION_DELAY * 2);
    }

    @Test(timeout = (long)(CONTAINER_CREATION_DELAY * 1.5))
    public void testAsync() throws Exception {
        Future<String> container1 = component.createContainerAsync("hello-world", null, new String[]{"ID=1", "DELAY="+CONTAINER_CREATION_DELAY}, null);
        Future<String> container2 = component.createContainerAsync("hello-world", null, new String[]{"ID=2", "DELAY="+CONTAINER_CREATION_DELAY}, null);
        Future<String> container3 = component.createContainerAsync("hello-world", null, new String[]{"ID=3", "DELAY=0"}, null);
        assertFalse("Asynchronously creating a container should take some time", container1.isDone());
        assertFalse("Asynchronously creating a container should take some time", container2.isDone());
        // (Third container is created without any delays.)
        String containerId1 = container1.get();
        String containerId2 = container2.get();
        String containerId3 = container3.get();
        assertEquals("IDs of asynchronously created containers", "1;2;3", containerId1 + ";" + containerId2 + ";" + containerId3);
    }

    protected static class DummyPlatformController extends AbstractDummyPlatformController {
        public DummyPlatformController(String sessionId, HobbitConfiguration configVar) {
            super(true);
            this.configuration = configVar;
            addCommandHeaderId(sessionId);
        }

        public void receiveCommand(byte command, byte[] data, String sessionId, AMQP.BasicProperties props) {
            if (command == Commands.DOCKER_CONTAINER_START) {
                String[] envVars = gson.fromJson(RabbitMQUtils.readString(data), StartCommandData.class).getEnvironmentVariables();
                String containerId = Stream.of(envVars).filter(kv -> kv.startsWith("ID=")).findAny().get().split("=", 2)[1];
                long delay = Long.parseLong(Stream.of(envVars).filter(kv -> kv.startsWith("DELAY=")).findAny().get().split("=", 2)[1]);
                LOGGER.info("Creating container {} with delay {}...", containerId, delay);

                try {
                    Thread.sleep(delay);

                    AMQP.BasicProperties.Builder propsBuilder = new AMQP.BasicProperties.Builder();
                    propsBuilder.deliveryMode(2);
                    propsBuilder.correlationId(props.getCorrelationId());
                    AMQP.BasicProperties replyProps = propsBuilder.build();

                    cmdChannel.basicPublish("", props.getReplyTo(), replyProps,
                            RabbitMQUtils.writeString(containerId));
                } catch (IOException | InterruptedException e) {
                    LOGGER.error("Exception in receiveCommand", e);
                }
            }
        }
    }
}
