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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.vocabulary.RDF;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.data.RabbitQueue;
import org.hobbit.core.rabbit.QueueingConsumer;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.vocab.HOBBIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Delivery;

/**
 * This abstract class implements basic functions that can be used to implement
 * a task generator.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public abstract class AbstractEvaluationModule extends AbstractPlatformConnectorComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractEvaluationModule.class);

    /**
     * Consumer used to receive the responses from the evaluation storage.
     */
    protected QueueingConsumer consumer;
    /**
     * Queue to the evaluation storage.
     */
    protected RabbitQueue evalModule2EvalStoreQueue;
    /**
     * Incoming queue from the evaluation storage.
     */
    protected RabbitQueue evalStore2EvalModuleQueue;
    /**
     * The URI of the experiment.
     */
    protected String experimentUri;
    /**
     * Timeout parameter for delivery queue message poll.
     */
    private static final int QUEUEPOLLTIMEOUT = 600000;

    public AbstractEvaluationModule() {
        defaultContainerType = Constants.CONTAINER_TYPE_BENCHMARK;
    }

    @Override
    public void init() throws Exception {
        super.init();

        // Get the experiment URI
        experimentUri = configuration.getString(Constants.HOBBIT_EXPERIMENT_URI_KEY, LOGGER);

        evalModule2EvalStoreQueue = getFactoryForOutgoingDataQueues().createDefaultRabbitQueue(
                generateSessionQueueName(Constants.EVAL_MODULE_2_EVAL_STORAGE_DEFAULT_QUEUE_NAME));
        evalStore2EvalModuleQueue = getFactoryForIncomingDataQueues().createDefaultRabbitQueue(
                generateSessionQueueName(Constants.EVAL_STORAGE_2_EVAL_MODULE_DEFAULT_QUEUE_NAME));

        consumer = new QueueingConsumer(evalStore2EvalModuleQueue.channel);
        evalStore2EvalModuleQueue.channel.basicConsume(evalStore2EvalModuleQueue.name, consumer);
    }

    @Override
    public void run() throws Exception {
        try {
            sendToCmdQueue(Commands.EVAL_MODULE_READY_SIGNAL);
            collectResponses();
            Model model = summarizeEvaluation();
            LOGGER.info("The result model has " + model.size() + " triples.");
            sendResultModel(model);
        } catch (Exception e) {
            throw reportAndWrap(e);
        }
    }

    /**
     * This method communicates with the evaluation storage to collect all response
     * pairs. For every pair the
     * {@link #evaluateResponse(byte[], byte[], long, long)} method is called.
     *
     * @throws Exception if a communication error occurs.
     */
    protected void collectResponses() throws Exception {
        byte[] expectedData;
        byte[] receivedData;
        long taskSentTimestamp;
        long responseReceivedTimestamp;

        BasicProperties props;
        byte requestBody[] = new byte[] { AbstractEvaluationStorage.NEW_ITERATOR_ID };
        ByteBuffer buffer;

        while (true) {
            // request next response pair
            props = new BasicProperties.Builder().deliveryMode(2).replyTo(evalStore2EvalModuleQueue.name).build();
            evalModule2EvalStoreQueue.channel.basicPublish("", evalModule2EvalStoreQueue.name, props, requestBody);
            // Wait for delivery message
            Delivery delivery = consumer.getDeliveryQueue().poll(QUEUEPOLLTIMEOUT, TimeUnit.MILLISECONDS);
            // parse the response
            if (delivery == null) {
                LOGGER.error("No Message Received after waiting for ten minutes");
                return;
            }

            buffer = ByteBuffer.wrap(delivery.getBody());

            // if the response is empty
            if (buffer.remaining() == 0) {
                LOGGER.error("Got a completely empty response from the evaluation storage.");
                return;
            }
            requestBody[0] = buffer.get();

            // if the response is empty
            if (buffer.remaining() == 0) {
                return;
            }
            byte[] data = RabbitMQUtils.readByteArray(buffer);
            taskSentTimestamp = data.length > 0 ? RabbitMQUtils.readLong(data) : 0;
            expectedData = RabbitMQUtils.readByteArray(buffer);

            data = RabbitMQUtils.readByteArray(buffer);
            responseReceivedTimestamp = data.length > 0 ? RabbitMQUtils.readLong(data) : 0;
            receivedData = RabbitMQUtils.readByteArray(buffer);

            evaluateResponse(expectedData, receivedData, taskSentTimestamp, responseReceivedTimestamp);

        }
    }

    /**
     * Evaluates the given response pair.
     *
     * @param expectedData              the data that has been expected
     * @param receivedData              the data that has been received from the
     *                                  system
     * @param taskSentTimestamp         the time at which the task has been sent to
     *                                  the system
     * @param responseReceivedTimestamp the time at which the response has been
     *                                  received from the system
     * @throws Exception if an error occurs during the evaluation
     */
    protected abstract void evaluateResponse(byte[] expectedData, byte[] receivedData, long taskSentTimestamp,
            long responseReceivedTimestamp) throws Exception;

    /**
     * Summarizes the evaluation and generates an RDF model containing the
     * evaluation results.
     *
     * @return an RDF model containing the evaluation results
     * @throws Exception if a sever error occurs
     */
    protected abstract Model summarizeEvaluation() throws Exception;

    /**
     * Sends the model to the benchmark controller.
     *
     * @param model the model that should be sent
     * @throws IOException if an error occurs during the commmunication
     */
    private void sendResultModel(Model model) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        model.write(outputStream, "JSONLD");
        sendToCmdQueue(Commands.EVAL_MODULE_FINISHED_SIGNAL, outputStream.toByteArray());
    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        super.receiveCommand(command, data);
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(evalModule2EvalStoreQueue);
        IOUtils.closeQuietly(evalStore2EvalModuleQueue);
        // if (evalModule2EvalStore != null) {
        // try {
        // evalModule2EvalStore.close();
        // } catch (Exception e) {
        // }
        // }
        super.close();
    }

    protected Model createDefaultModel() {
        Model resultModel = ModelFactory.createDefaultModel();
        resultModel.add(resultModel.createResource(experimentUri), RDF.type, HOBBIT.Experiment);
        return resultModel;
    }
}
