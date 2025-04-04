package org.hobbit.core.mimic;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.TestConstants;
import org.hobbit.core.components.AbstractComponent;
import org.hobbit.core.components.AbstractPlatformConnectorComponent;
import org.hobbit.core.components.dummy.AbstractDummyPlatformController;
import org.hobbit.core.components.dummy.DummyComponentExecutor;
import org.hobbit.core.data.StartCommandData;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.core.rabbit.SimpleFileSender;
import org.hobbit.utils.config.HobbitConfiguration;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.gson.Gson;
import com.rabbitmq.client.AMQP;

/**
 * This is a test that simulates the workflow of the
 * {@link DockerBasedMimickingAlg} class simulating the platform controller that
 * would normally create a docker container.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class DockerBasedMimickingAlgTest extends AbstractPlatformConnectorComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(DockerBasedMimickingAlgTest.class);

    private static final String HOBBIT_SESSION_ID = "123";
    private static final String MIMICKING_ALGORITHM_DOCKER_IMAGE = "mimickingAlg";
    private static final int DATA_SIZE = 1000;
    private static final String DATA_SIZE_KEY = "DATA_SIZE";
    private static final String OUTPUT_FILE_NAME = "output.txt";

    

    private File outputDir;

    @Test(timeout = 30000)
    public void test() throws Exception {
    	Configuration configurationVar = new PropertiesConfiguration();
    	configurationVar.addProperty(Constants.RABBIT_MQ_HOST_NAME_KEY, TestConstants.RABBIT_HOST);
    	configurationVar.addProperty(Constants.HOBBIT_SESSION_ID_KEY, HOBBIT_SESSION_ID);
    	configuration = new HobbitConfiguration();
    	configuration.addConfiguration(configurationVar);
        outputDir = generateTempDir();
        LOGGER.debug("File will be writte to {}", outputDir.getAbsolutePath());
        // start platform controller
        LOGGER.debug("Creating controller and waiting for it to be ready...");
        DummyPlatformController platform = new DummyPlatformController(HOBBIT_SESSION_ID,configuration);
        DummyComponentExecutor platformExecutor = new DummyComponentExecutor(platform);
        Thread platformThread = new Thread(platformExecutor);
        platformThread.start();
        platform.waitForControllerBeingReady();
        // init this class
        LOGGER.debug("Initializing...");
        init();
        // run this class
        LOGGER.debug("Running...");
        run();
        // shutdown the platform
        LOGGER.debug("Shutting down the platform...");
        platform.terminate();
        platformThread.join();

        // check received data
        List<String> receivedLines = FileUtils
                .readLines(new File(outputDir.getAbsolutePath() + File.separatorChar + OUTPUT_FILE_NAME));
        Assert.assertEquals(DATA_SIZE, receivedLines.size());
        for (int i = 0; i < DATA_SIZE; ++i) {
            Assert.assertEquals(Integer.toString(i), receivedLines.get(i));
        }
    }

    @Override
    public void run() throws Exception {
        // start mimicking
        DockerBasedMimickingAlg algorithm = new DockerBasedMimickingAlg(this, MIMICKING_ALGORITHM_DOCKER_IMAGE);
        algorithm.generateData(outputDir.getAbsolutePath(),
                new String[] { DATA_SIZE_KEY + "=" + Integer.toString(DATA_SIZE) });
    }

    private File generateTempDir() throws IOException {
        File temp = File.createTempFile("test", "test");
        temp.delete();
        temp.mkdir();
        return temp;
    }

    protected static class DummyPlatformController extends AbstractDummyPlatformController {

        public DummyComponentExecutor mimickingExecutor;
        public Thread mimickingThread;
        public Random random = new Random();
        private Gson gson = new Gson();

        public DummyPlatformController(String sessionId) {
            super();
            addCommandHeaderId(sessionId);
        }
        public DummyPlatformController(String sessionId, HobbitConfiguration c) {
            super();
            addCommandHeaderId(sessionId);
            configuration = c;
        }

        public void receiveCommand(byte command, byte[] data, String sessionId, AMQP.BasicProperties props) {
            String replyTo = null;
            if (props != null) {
                replyTo = props.getReplyTo();
            }

            LOGGER.info("received command: session={}, command={}, data={}", sessionId, Commands.toString(command),
                    data != null ? RabbitMQUtils.readString(data) : "null");
            if (command == Commands.DOCKER_CONTAINER_START) {
                try {
                    String startCommandJson = RabbitMQUtils.readString(data);
                    StartCommandData startCommand = gson.fromJson(startCommandJson, StartCommandData.class);
                    final String containerId = Integer.toString(random.nextInt());

                    if (startCommand.image.equals(MIMICKING_ALGORITHM_DOCKER_IMAGE)) {
                        // Create the mimicking Algorithm
                        mimickingExecutor = new DummyComponentExecutor(
                                new DummyMimickingAlgorithm(startCommand.environmentVariables, configuration)) {
                            @Override
                            public void run() {
                                super.run();
                                // Send the message that the mimicking algorithm
                                // terminated
                                try {
                                    sendToCmdQueue(Constants.HOBBIT_SESSION_ID_FOR_BROADCASTS,
                                            Commands.DOCKER_CONTAINER_TERMINATED,
                                            RabbitMQUtils.writeByteArrays(null,
                                                    new byte[][] { RabbitMQUtils.writeString(containerId) },
                                                    new byte[] { (byte) 0 }),
                                            null);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    success = false;
                                }
                            }
                        };
                        mimickingThread = new Thread(mimickingExecutor);
                        mimickingThread.start();

                        AMQP.BasicProperties.Builder propsBuilder = new AMQP.BasicProperties.Builder();
                        propsBuilder.deliveryMode(2);
                        propsBuilder.correlationId(props.getCorrelationId());
                        AMQP.BasicProperties replyProps = propsBuilder.build();

                        cmdChannel.basicPublish("", replyTo, replyProps,
                                RabbitMQUtils.writeString(containerId));
                    } else {
                        LOGGER.error("Got unknown start command. Ignoring it.");
                    }
                } catch (IOException e) {
                    LOGGER.error("Exception while trying to respond to a container creation command.", e);
                }
            }
        }
    }

    protected static class DummyMimickingAlgorithm extends AbstractComponent {

        private String env[];
        private int dataSize;
        private SimpleFileSender sender;

        public DummyMimickingAlgorithm(String[] env) {
            super();
            this.env = env;
        }
        public DummyMimickingAlgorithm(String[] env, HobbitConfiguration c) {
            super();
            this.env = env;
            configuration = c;
        }

        @Override
        public void init() throws Exception {
            super.init();
            String queueName = null;
            for (String variable : env) {
                if (variable.contains(Constants.DATA_QUEUE_NAME_KEY)) {
                    queueName = variable.replace(Constants.DATA_QUEUE_NAME_KEY + "=", "");
                } else if (variable.contains(DATA_SIZE_KEY)) {
                    dataSize = Integer.parseInt(variable.replace(DATA_SIZE_KEY + "=", ""));
                }
            }
            Objects.requireNonNull(queueName);
            // create the sender
            sender = SimpleFileSender.create(outgoingDataQueuefactory, queueName);
        }

        @Override
        public void run() throws Exception {
            LOGGER.info("Mimicking algorithm started...");
            InputStream is = null;
            try {
                // create input stream, e.g., by opening a file
                final PipedOutputStream out = new PipedOutputStream();
                is = new PipedInputStream(out);
                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            byte[] linebreak = RabbitMQUtils.writeString(String.format("%n"));
                            for (int i = 0; i < dataSize; ++i) {
                                out.write(RabbitMQUtils.writeString(Integer.toString(i)));
                                out.write(linebreak);
                            }
                        } catch (Exception e) {
                            LOGGER.error("Error while generating data.", e);
                        } finally {
                            IOUtils.closeQuietly(out);
                        }
                    }
                });
                t.start();
                // send data
                sender.streamData(is, OUTPUT_FILE_NAME);
            } catch (Exception e) {
                LOGGER.error("Error while generating data.", e);
            } finally {
                IOUtils.closeQuietly(is);
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
            // close the sender
            IOUtils.closeQuietly(sender);
        }
    }
}
