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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.vocab.HOBBIT;
import org.hobbit.vocab.HobbitErrors;
import org.hobbit.vocab.HobbitExperiments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class implements basic methods for a benchmark controller.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public abstract class AbstractBenchmarkController extends AbstractPlatformConnectorComponent {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractBenchmarkController.class);

    protected static final String DEFAULT_EVAL_STORAGE_IMAGE = "hobbitproject/defaultevaluationstorage:1.0.9";
    protected static final String[] DEFAULT_EVAL_STORAGE_PARAMETERS = new String[] { "HOBBIT_RIAK_NODES=1" };

    /**
     * The benchmark result as RDF model received from the evaluation module.
     */
    protected Model resultModel;
    /**
     * The benchmark result as RDF model received from the evaluation module.
     */
    protected Semaphore resultModelMutex = new Semaphore(1);
    /**
     * Mutex used to wait for the start signal from the controller.
     */
    protected Semaphore startBenchmarkMutex = new Semaphore(0);
    /**
     * Mutex used to wait for the data generators to be ready.
     */
    protected Semaphore dataGenReadyMutex = new Semaphore(0);
    /**
     * Mutex used to wait for the task generators to be ready.
     */
    protected Semaphore taskGenReadyMutex = new Semaphore(0);
    /**
     * Mutex used to wait for the evaluation storage to be ready.
     */
    protected Semaphore evalStoreReadyMutex = new Semaphore(0);
    /**
     * Mutex used to wait for the start signal for this benchmark.
     */
    protected Semaphore benchmarkStartSignalMutex = new Semaphore(0);
    /**
     * Mutex used to wait for the data generators to terminate.
     */
    protected Semaphore dataGenTerminatedMutex = new Semaphore(0);
    /**
     * Mutex used to wait for the task generators to terminate.
     */
    protected Semaphore taskGenTerminatedMutex = new Semaphore(0);
    /**
     * Mutex used to wait for the benchmarked system to terminate.
     */
    protected Semaphore systemTerminatedMutex = new Semaphore(0);
    /**
     * Mutex used to wait for the evaluation storage to terminate.
     */
    protected Semaphore evalStoreTerminatedMutex = new Semaphore(0);
    /**
     * Mutex used to wait for the evaluation module to terminate.
     */
    protected Semaphore evalModuleTerminatedMutex = new Semaphore(0);
    /**
     * The set of data generator container ids.
     */
    protected Set<String> dataGenContainerIds = new HashSet<String>();
    /**
     * The set of task generator container ids.
     */
    protected Set<String> taskGenContainerIds = new HashSet<String>();
    /**
     * The container id of the evaluation storage.
     */
    protected String evalStoreContainerId;
    /**
     * The container id of the evaluation module.
     */
    protected String evalModuleContainerId;
    /**
     * The container id of the benchmarked system.
     */
    protected String systemContainerId = null;
    /**
     * The exit code of the system container
     */
    protected int systemExitCode = 0;
    /**
     * The RDF model containing the benchmark parameters.
     */
    protected Model benchmarkParamModel;
    /**
     * The URI of the experiment.
     */
    protected String experimentUri;

    /**
     * Constructor.
     */
    public AbstractBenchmarkController() {
        defaultContainerType = Constants.CONTAINER_TYPE_BENCHMARK;
    }

    @Override
    public void init() throws Exception {
        super.init();
        // benchmark controllers should be able to accept broadcasts
        addCommandHeaderId(Constants.HOBBIT_SESSION_ID_FOR_BROADCASTS);

        // Get the benchmark parameter model
        benchmarkParamModel = configuration.getModel(Constants.BENCHMARK_PARAMETERS_MODEL_KEY, LOGGER);
        // Get the experiment URI
        experimentUri = configuration.getString(Constants.HOBBIT_EXPERIMENT_URI_KEY, LOGGER);
    }

    @Override
    public void run() throws Exception {
        try {
            sendToCmdQueue(Commands.BENCHMARK_READY_SIGNAL);
            // wait for the start signal
            startBenchmarkMutex.acquire();
            executeBenchmark();
        } catch (Exception e) {
            throw reportAndWrap(e);
        }
    }

    protected abstract void executeBenchmark() throws Exception;

    /**
     * Creates the given number of data generators using the given image name and
     * environment variables.
     *
     * @param dataGeneratorImageName name of the data generator Docker image
     * @param numberOfDataGenerators number of generators that should be created
     * @param envVariables           environment variables for the data generators
     */
    protected void createDataGenerators(String dataGeneratorImageName, int numberOfDataGenerators,
            String[] envVariables) {
        createGenerator(dataGeneratorImageName, numberOfDataGenerators, envVariables, dataGenContainerIds);
    }

    /**
     * Creates the given number of task generators using the given image name and
     * environment variables.
     *
     * @param taskGeneratorImageName name of the task generator Docker image
     * @param numberOfTaskGenerators number of generators that should be created
     * @param envVariables           environment variables for the task generators
     */
    protected void createTaskGenerators(String taskGeneratorImageName, int numberOfTaskGenerators,
            String[] envVariables) {
        createGenerator(taskGeneratorImageName, numberOfTaskGenerators, envVariables, taskGenContainerIds);
    }

    /**
     * Internal method for creating generator components.
     *
     * @param generatorImageName name of the generator Docker image
     * @param numberOfGenerators number of generators that should be created
     * @param envVariables       environment variables for the task generators
     * @param generatorIds       set of generator container names
     */
    private void createGenerator(String generatorImageName, int numberOfGenerators, String[] envVariables,
            Set<String> generatorIds) {
        String containerId;
        String variables[] = envVariables != null ? Arrays.copyOf(envVariables, envVariables.length + 2)
                : new String[2];
        // NOTE: Count only includes generators created within this method call.
        variables[variables.length - 2] = Constants.GENERATOR_COUNT_KEY + "=" + numberOfGenerators;
        for (int i = 0; i < numberOfGenerators; ++i) {
            // At the start generatorIds is empty, and new generators are added to it
            // immediately.
            // Current size of that set is used to make IDs for new generators.
            variables[variables.length - 1] = Constants.GENERATOR_ID_KEY + "=" + generatorIds.size();
            containerId = createContainer(generatorImageName, variables);
            if (containerId != null) {
                generatorIds.add(containerId);
            } else {
                String errorMsg = "Couldn't create generator component. Aborting.";
                LOGGER.error(errorMsg);
                throw new IllegalStateException(errorMsg);
            }
        }
    }

    /**
     * Creates the evaluate module using the given image name and environment
     * variables.
     *
     * @param evalModuleImageName name of the evaluation module image
     * @param envVariables        environment variables that should be given to the
     *                            module
     */
    protected void createEvaluationModule(String evalModuleImageName, String[] envVariables) {
        envVariables = ArrayUtils.add(envVariables, Constants.HOBBIT_EXPERIMENT_URI_KEY + "=" + experimentUri);
        evalModuleContainerId = createContainer(evalModuleImageName, envVariables);
        if (evalModuleContainerId == null) {
            String errorMsg = "Couldn't create evaluation module. Aborting.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }
    }

    /**
     * Creates the default evaluation storage using the given image name and
     * environment variables.
     */
    protected void createEvaluationStorage() {
        String[] envVariables = ArrayUtils.add(DEFAULT_EVAL_STORAGE_PARAMETERS,
                Constants.RABBIT_MQ_HOST_NAME_KEY + "=" + this.rabbitMQHostName);
        createEvaluationStorage(DEFAULT_EVAL_STORAGE_IMAGE, envVariables);
    }

    /**
     * Creates the evaluate storage using the given image name and environment
     * variables.
     *
     * @param evalStorageImageName name of the evaluation storage image
     * @param envVariables         environment variables that should be given to the
     *                             component
     */
    protected void createEvaluationStorage(String evalStorageImageName, String[] envVariables) {
        evalStoreContainerId = createContainer(evalStorageImageName, Constants.CONTAINER_TYPE_DATABASE, envVariables);
        if (evalStoreContainerId == null) {
            String errorMsg = "Couldn't create evaluation storage. Aborting.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }
    }

    /**
     * This method waits for the data generators, task generators and evaluation
     * storage to send their ready signals.
     */
    protected void waitForComponentsToInitialize() {
        LOGGER.debug("Waiting for {} Data Generators to be ready.", dataGenContainerIds.size());
        try {
            dataGenReadyMutex.acquire(dataGenContainerIds.size());
        } catch (InterruptedException e) {
            String errorMsg = "Interrupted while waiting for the data generators to be ready.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }
        LOGGER.debug("Waiting for {} Task Generators to be ready.", taskGenContainerIds.size());
        try {
            taskGenReadyMutex.acquire(taskGenContainerIds.size());
        } catch (InterruptedException e) {
            String errorMsg = "Interrupted while waiting for the task generators to be ready.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }
        if (evalStoreContainerId != null) {
            LOGGER.debug("Waiting for Evaluation Storage to be ready.");
            try {
                evalStoreReadyMutex.acquire();
            } catch (InterruptedException e) {
                String errorMsg = "Interrupted while waiting for the evaluation storage to be ready.";
                LOGGER.error(errorMsg);
                throw new IllegalStateException(errorMsg, e);
            }
        }
    }

    /**
     * Waits for the termination of all data generators.
     */
    protected void waitForDataGenToFinish() {
        LOGGER.debug("Waiting for {} Data Generators to finish.", dataGenContainerIds.size());
        try {
            dataGenTerminatedMutex.acquire(dataGenContainerIds.size());
        } catch (InterruptedException e) {
            String errorMsg = "Interrupted while waiting for the data generators to terminate.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }
        try {
            sendToCmdQueue(Commands.DATA_GENERATION_FINISHED);
        } catch (IOException e) {
            String errorMsg = "Couldn't send the " + Commands.DATA_GENERATION_FINISHED + " command. Aborting.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }
    }

    /**
     * Waits for the termination of all task generators.
     */
    protected void waitForTaskGenToFinish() {
        LOGGER.debug("Waiting for {} Task Generators to finish.", dataGenContainerIds.size());
        try {
            taskGenTerminatedMutex.acquire(taskGenContainerIds.size());
        } catch (InterruptedException e) {
            String errorMsg = "Interrupted while waiting for the task generators to terminate.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }
        try {
            sendToCmdQueue(Commands.TASK_GENERATION_FINISHED);
        } catch (IOException e) {
            String errorMsg = "Couldn't send the " + Commands.TASK_GENERATION_FINISHED + " command. Aborting.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }
    }

    /**
     * This method waits for the benchmarked system to terminate or times out after
     * the given amount of time (in milliseconds).
     *
     * @param maxWaitingTime maximum waiting time in milliseconds
     * @return {@code true} if the system has been terminated or {@code false} if
     *         the method timed out
     */
    protected boolean waitForSystemToFinish(long maxWaitingTime) {
        LOGGER.debug("Waiting for the benchmarked system to finish.");
        try {
            if (systemTerminatedMutex.tryAcquire(1, maxWaitingTime, TimeUnit.MILLISECONDS)) {
                return true;
            } else {
                LOGGER.warn(
                        "Didn't got a message that the system has been terminated. Stopped waiting after {} milliseconds.",
                        maxWaitingTime);
                return false;
            }
        } catch (InterruptedException e) {
            String errorMsg = "Interrupted while waiting for the system to terminate.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }
    }

    /**
     * This method waits for the benchmarked system to terminate.
     */
    protected void waitForSystemToFinish() {
        LOGGER.debug("Waiting for the benchmarked system to finish.");
        try {
            systemTerminatedMutex.acquire();
        } catch (InterruptedException e) {
            String errorMsg = "Interrupted while waiting for the system to terminate.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }
    }

    /**
     * Waits for the termination of the evaluation module and the evaluation
     * storage.
     */
    protected void waitForEvalComponentsToFinish() {
        LOGGER.debug("Waiting for the evaluation module to finish.");
        try {
            evalModuleTerminatedMutex.acquire();
        } catch (InterruptedException e) {
            String errorMsg = "Interrupted while waiting for the evaluation module to terminate.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        }
        if (evalStoreContainerId != null) {
            LOGGER.debug("Waiting for the evaluation storage to finish.");
            try {
                evalStoreTerminatedMutex.acquire();
            } catch (InterruptedException e) {
                String errorMsg = "Interrupted while waiting for the evaluation storage to terminate.";
                LOGGER.error(errorMsg);
                throw new IllegalStateException(errorMsg, e);
            }
        }
    }

    /**
     * Uses the given model as result model if the result model is
     * <code>null</code>. Else, the two models are merged.
     *
     * @param resultModel the new result model
     */
    protected void setResultModel(Model resultModel) {
        try {
            resultModelMutex.acquire();
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for the result model mutex. Returning.", e);
        }
        try {
            if (this.resultModel == null) {
                this.resultModel = resultModel;
            } else {
                this.resultModel.add(resultModel);
            }
        } finally {
            resultModelMutex.release();
        }
        addParametersToResultModel();
    }

    /**
     * Generates a default model containing an error code and the benchmark
     * parameters if no result model has been received from the evaluation module
     * until now. If the model already has been received, the error is added to the
     * existing model.
     */
    protected void generateErrorResultModel() {
        try {
            resultModelMutex.acquire();
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for the result model mutex. Returning.", e);
        }
        try {
            if (resultModel == null) {
                this.resultModel = ModelFactory.createDefaultModel();
                resultModel.add(resultModel.getResource(experimentUri), RDF.type, HOBBIT.Experiment);
            }
            resultModel.add(resultModel.getResource(experimentUri), HOBBIT.terminatedWithError,
                    HobbitErrors.BenchmarkCrashed);
        } finally {
            resultModelMutex.release();
        }
        addParametersToResultModel();
    }

    /**
     * Adds the {@link #benchmarkParamModel} triples to the {@link #resultModel} .
     */
    protected void addParametersToResultModel() {
        try {
            resultModelMutex.acquire();
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for the result model mutex. Returning.", e);
        }
        try {
            Resource experimentResource = resultModel.getResource(experimentUri);
            StmtIterator iterator = benchmarkParamModel.listStatements(HobbitExperiments.New, null, (RDFNode) null);
            Statement statement;
            while (iterator.hasNext()) {
                statement = iterator.next();
                resultModel.add(experimentResource, statement.getPredicate(), statement.getObject());
            }
        } finally {
            resultModelMutex.release();
        }
    }

    /**
     * Sends the result RDF model to the platform controller.
     *
     * @param model model containing the results
     */
    protected void sendResultModel(Model model) {
        try {
            resultModelMutex.acquire();
        } catch (InterruptedException e) {
            LOGGER.error("Interrupted while waiting for the result model mutex. Returning.", e);
        }
        try {
            if (systemExitCode != 0) {
                model.add(model.getResource(experimentUri), HOBBIT.terminatedWithError, HobbitErrors.SystemCrashed);
            }
            sendToCmdQueue(Commands.BENCHMARK_FINISHED_SIGNAL, RabbitMQUtils.writeModel(model));
        } catch (IOException e) {
            String errorMsg = "Exception while trying to send the result to the platform controller.";
            LOGGER.error(errorMsg);
            throw new IllegalStateException(errorMsg, e);
        } finally {
            resultModelMutex.release();
        }
    }

    @Override
    public void receiveCommand(byte command, byte[] data) {
        switch (command) {
        case Commands.START_BENCHMARK_SIGNAL: {
            startBenchmarkMutex.release();
            systemContainerId = RabbitMQUtils.readString(data);
            break;
        }
        case Commands.DATA_GENERATOR_READY_SIGNAL: {
            LOGGER.debug("Received DATA_GENERATOR_READY_SIGNAL");
            dataGenReadyMutex.release();
            break;
        }
        case Commands.TASK_GENERATOR_READY_SIGNAL: {
            LOGGER.debug("Received TASK_GENERATOR_READY_SIGNAL");
            taskGenReadyMutex.release();
            break;
        }
        case Commands.EVAL_STORAGE_READY_SIGNAL: {
            LOGGER.debug("Received EVAL_STORAGE_READY_SIGNAL");
            evalStoreReadyMutex.release();
            break;
        }
        case Commands.DOCKER_CONTAINER_TERMINATED: {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            String containerName = RabbitMQUtils.readString(buffer);
            int exitCode = buffer.get();
            containerTerminated(containerName, exitCode);
            break;
        }
        case Commands.EVAL_MODULE_FINISHED_SIGNAL: {
            setResultModel(RabbitMQUtils.readModel(data));
            LOGGER.info("model size = " + resultModel.size());
            break;
        }
        }
        super.receiveCommand(command, data);
    }

    /**
     * This method handles messages from the command bus containing the information
     * that a container terminated. It checks whether the container belongs to the
     * current benchmark and whether it has to react.
     *
     * @param containerName the name of the terminated container
     * @param exitCode      the exit code of the terminated container
     */
    protected void containerTerminated(String containerName, int exitCode) {
        if (dataGenContainerIds.contains(containerName)) {
            if (exitCode == 0) {
                dataGenTerminatedMutex.release();
            } else {
                containerCrashed(containerName);
            }
        } else if (taskGenContainerIds.contains(containerName)) {
            if (exitCode == 0) {
                taskGenTerminatedMutex.release();
            } else {
                containerCrashed(containerName);
            }
        } else if (containerName.equals(evalStoreContainerId)) {
            if (exitCode == 0) {
                evalStoreTerminatedMutex.release();
            } else {
                containerCrashed(containerName);
            }
        } else if (containerName.equals(systemContainerId)) {
            systemTerminatedMutex.release();
            systemExitCode = exitCode;
        } else if (containerName.equals(evalModuleContainerId)) {
            if (exitCode == 0) {
                evalModuleTerminatedMutex.release();
                try {
                    sendToCmdQueue(Commands.EVAL_STORAGE_TERMINATE);
                } catch (IOException e) {
                    LOGGER.error("Couldn't send the " + Commands.EVAL_STORAGE_TERMINATE
                            + " command. Won't wait for the evaluation store to terminate!", e);
                    evalStoreTerminatedMutex.release();
                }
            } else {
                containerCrashed(containerName);
            }
        }
    }

    protected void containerCrashed(String containerName) {
        LOGGER.error("A component crashed (\"{}\"). Terminating.", containerName);
        generateErrorResultModel();
        sendResultModel(resultModel);
        System.exit(1);
    }
}
