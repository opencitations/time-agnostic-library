package org.hobbit.benchmark.versioning.components;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.jena.rdf.model.NodeIterator;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.benchmark.versioning.util.SystemAdapterConstants;
import org.hobbit.core.Commands;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractBenchmarkController;
import org.hobbit.core.components.utils.SystemResourceUsageRequester;
import org.hobbit.core.data.usage.ResourceUsageInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersioningBenchmarkController extends AbstractBenchmarkController {

	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningBenchmarkController.class);

	private static final String DATA_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningdatagenerator:2.2.1";
	private static final String TASK_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningtaskgenerator:2.2.1";
	private static final String EVALUATION_MODULE_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/papv/versioningevaluationmodule:2.2.1";
	
	private static final String PREFIX = "http://w3id.org/hobbit/versioning-benchmark/vocab#";
	
    private String[] evalModuleEnvVariables = null;
    private String[] dataGenEnvVariables = null;
    private String[] evalStorageEnvVariables = null;
    
    private Semaphore versionSentMutex = new Semaphore(0);
    private Semaphore versionLoadedMutex = new Semaphore(0);
        
    private int numberOfDataGenerators;
    private int numOfVersions;
    private int loadedVersion = 0;
    private long prevLoadingStartedTime = 0;
    private long[] loadingTimes;
    private AtomicIntegerArray triplesToBeAdded;
    private AtomicIntegerArray triplesToBeDeleted;
    private AtomicIntegerArray triplesToBeLoaded;
    private AtomicInteger numberOfMessages = new AtomicInteger(0);
    
    private SystemResourceUsageRequester resUsageRequester = null;
    private long systemInitialUsableSpace = 0;
    private long systemStorageSpaceCost = 0;
        
	@Override
	public void init() throws Exception {
        LOGGER.info("Initilalizing Benchmark Controller...");
        super.init();
               
		numberOfDataGenerators = (Integer) getPropertyOrDefault(PREFIX + "hasNumberOfGenerators", 1);
		int v0Size =  (Integer) getPropertyOrDefault(PREFIX + "v0SizeInTriples", 1000000);
		int generatorSeed = (Integer) getPropertyOrDefault(PREFIX + "generatorSeed", 0);
		numOfVersions =  (Integer) getPropertyOrDefault(PREFIX + "numberOfVersions", 12);
		int insRatio = (Integer) getPropertyOrDefault(PREFIX + "versionInsertionRatio", 5);
		int delRatio = (Integer) getPropertyOrDefault(PREFIX + "versionDeletionRatio", 3);
		String dataForm = (String) getPropertyOrDefault(PREFIX + "generatedDataForm", "ic");
		String enabledQueries = (String) getPropertyOrDefault(PREFIX + "enableDisableQT", "QT1=1, QT2=1, QT3=1, QT4=1, QT5=1, QT6=1, QT7=1, QT8=1");
		
		loadingTimes = new long[numOfVersions];
		triplesToBeAdded = new AtomicIntegerArray(numOfVersions);
		triplesToBeDeleted = new AtomicIntegerArray(numOfVersions);
		triplesToBeLoaded = new AtomicIntegerArray(numOfVersions);
		
		// data generators environmental values
		dataGenEnvVariables = new String[] {
				VersioningConstants.NUMBER_OF_DATA_GENERATORS + "=" + numberOfDataGenerators,
				VersioningConstants.DATA_GENERATOR_SEED + "=" + generatorSeed,
				VersioningConstants.V0_SIZE_IN_TRIPLES + "=" + v0Size,
				VersioningConstants.NUMBER_OF_VERSIONS + "=" + numOfVersions,
				VersioningConstants.VERSION_INSERTION_RATIO + "=" + insRatio,
				VersioningConstants.VERSION_DELETION_RATIO + "=" + delRatio,
				VersioningConstants.SENT_DATA_FORM + "=" + dataForm,
				VersioningConstants.ENABLED_QUERY_TYPES + "=" + enabledQueries
		};
		LOGGER.info(enabledQueries);
		
		// evaluation module environmental values
		evalModuleEnvVariables = new String[] {
				VersioningConstants.INITIAL_VERSION_INGESTION_SPEED + "=" + PREFIX + "initialVersionIngestionSpeed",
				VersioningConstants.AVG_APPLIED_CHANGES_PS + "=" + PREFIX + "avgAppliedChangesPS",
				VersioningConstants.STORAGE_COST + "=" + PREFIX + "storageCost",
				VersioningConstants.QT_1_AVG_EXEC_TIME + "=" + PREFIX + "queryType1AvgExecTime",
				VersioningConstants.QT_2_AVG_EXEC_TIME + "=" + PREFIX + "queryType2AvgExecTime",
				VersioningConstants.QT_3_AVG_EXEC_TIME + "=" + PREFIX + "queryType3AvgExecTime",
				VersioningConstants.QT_4_AVG_EXEC_TIME + "=" + PREFIX + "queryType4AvgExecTime",
				VersioningConstants.QT_5_AVG_EXEC_TIME + "=" + PREFIX + "queryType5AvgExecTime",
				VersioningConstants.QT_6_AVG_EXEC_TIME + "=" + PREFIX + "queryType6AvgExecTime",
				VersioningConstants.QT_7_AVG_EXEC_TIME + "=" + PREFIX + "queryType7AvgExecTime",
				VersioningConstants.QT_8_AVG_EXEC_TIME + "=" + PREFIX + "queryType8AvgExecTime",
				VersioningConstants.QUERY_FAILURES + "=" + PREFIX + "queryFailures",
				VersioningConstants.QUERIES_PER_SECOND + "=" + PREFIX + "queriesPerSecond"
		};
		
		evalStorageEnvVariables = ArrayUtils.add(DEFAULT_EVAL_STORAGE_PARAMETERS,
                Constants.RABBIT_MQ_HOST_NAME_KEY + "=" + this.rabbitMQHostName);
		evalStorageEnvVariables = ArrayUtils.add(evalStorageEnvVariables, "ACKNOWLEDGEMENT_FLAG=true");
		
		// Create data generators
		createDataGenerators(DATA_GENERATOR_CONTAINER_IMAGE, numberOfDataGenerators, dataGenEnvVariables);
		LOGGER.info("Data Generators created successfully.");

		// Create task generators
		createTaskGenerators(TASK_GENERATOR_CONTAINER_IMAGE, 1, new String[] {} );
		LOGGER.info("Task Generators created successfully.");
		
		// Create evaluation storage
		createEvaluationStorage(DEFAULT_EVAL_STORAGE_IMAGE, evalStorageEnvVariables);
		LOGGER.info("Evaluation Storage created successfully.");
		
		waitForComponentsToInitialize();
		LOGGER.info("All components initilized.");
	}
	
	/**
     * A generic method for loading parameters from the benchmark parameter model
     * 
     * @param property		the property that we want to load
     * @param defaultValue	the default value that will be used in case of an error while loading the property
     * @return				the value of requested parameter
     */
	@SuppressWarnings("unchecked")
	private <T> T getPropertyOrDefault(String property, T defaultValue) {
		T propertyValue = null;
		NodeIterator iterator = benchmarkParamModel
				.listObjectsOfProperty(benchmarkParamModel
		        .getProperty(property));

		if (iterator.hasNext()) {
			try {
				if (defaultValue instanceof String) {
					if(((String) defaultValue).equals("ic")) {
						Properties serializationFormats = new Properties();
						try {
							serializationFormats.load(ClassLoader.getSystemResource("data_forms.properties").openStream());
						} catch (IOException e) {
							LOGGER.error("Exception while parsing available data forms.");
						}
						return (T) serializationFormats.getProperty(iterator.next().asResource().getLocalName());
					} else {
						return (T) iterator.next().asLiteral().getString(); 
					}
				} else if (defaultValue instanceof Integer) {
					return (T) ((Integer) iterator.next().asLiteral().getInt());
				} else if (defaultValue instanceof Long) {
					return (T) ((Long) iterator.next().asLiteral().getLong());
				} else if (defaultValue instanceof Double) {
					return (T) ((Double) iterator.next().asLiteral().getDouble());
				}
            } catch (Exception e) {
            	LOGGER.error("Exception while parsing parameter.");
            }
		} else {
			LOGGER.info("Couldn't get property '" + property + "' from the parameter model. Using '" + defaultValue + "' as a default value.");
			propertyValue = defaultValue;
		}
		return propertyValue;
	}
	
	@Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == VersioningConstants.DATA_GEN_VERSION_DATA_SENT) {
        	// TODO: 
        	// triplesToBeLoaded information have to be sent by the external component 
        	// computing the gold standard. Only the number of messages and data generator's
        	// id will be sent with DATA_GEN_VERSION_SENT command
        	ByteBuffer buffer = ByteBuffer.wrap(data);
        	triplesToBeAdded.addAndGet(loadedVersion, buffer.getInt());
        	triplesToBeDeleted.addAndGet(loadedVersion, buffer.getInt());
        	triplesToBeLoaded.addAndGet(loadedVersion, buffer.getInt());
            int dataGeneratorId = buffer.getInt();
            int dataGenNumOfMessages = buffer.getInt();
        	numberOfMessages.addAndGet(dataGenNumOfMessages);
        	
        	// signal sent from data generator that all its data generated successfully
        	LOGGER.info("Recieved signal from Data Generator " + dataGeneratorId + " that all data (#" + dataGenNumOfMessages + ") of version " + loadedVersion + " successfully sent to the system.");
        	versionSentMutex.release();
	    } else if (command == SystemAdapterConstants.BULK_LOADING_DATA_FINISHED) {
            // signal sent from the system that a version loaded successfully
	    	LOGGER.info("Recieved signal that all data of version " + loadedVersion + " successfully loaded by the system.");
        	long currTimeMillis = System.currentTimeMillis();
        	long versionLoadingTime = currTimeMillis - prevLoadingStartedTime;
        	loadingTimes[loadedVersion++] = versionLoadingTime;
        	versionLoadedMutex.release();
        }
        super.receiveCommand(command, data);
    }
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.hobbit.core.components.AbstractBenchmarkController#executeBenchmark()
	 */
	@Override
	protected void executeBenchmark() throws Exception {
		// give the start signals
        sendToCmdQueue(Commands.TASK_GENERATOR_START_SIGNAL);
        sendToCmdQueue(Commands.DATA_GENERATOR_START_SIGNAL);
		LOGGER.info("Start signals sent to Data and Task Generators");

		LOGGER.info("Creating requester for system resource usage information.");
		resUsageRequester = SystemResourceUsageRequester.create(this, getHobbitSessionId());

		LOGGER.info("Measuring system's usable space before data loading");
		ResourceUsageInformation infoBefore = resUsageRequester.getSystemResourceUsage();
		if (infoBefore.getDiskStats() != null) {
			systemInitialUsableSpace = infoBefore.getDiskStats().getFsSizeSum();
			LOGGER.info("System's usable space before data loading: " + systemInitialUsableSpace);
		} else {
			LOGGER.info(infoBefore.toString());
			LOGGER.info("Got null as response.");
		}
		
		// iterate through different versions starting from version 0
		for (int v = 0; v < numOfVersions; v++) {			
			// wait for all data generators to sent data of version v to system adapter
			LOGGER.info("Waiting for all data generators to send data of version " + v + " to the system.");
			versionSentMutex.acquire(numberOfDataGenerators);
			LOGGER.info("Signal from all data generators received.");
			
			// Send signal that all data, generated and sent to the system successfully.
			// The number of messages along with a flag is also sent
			LOGGER.info("Send signal to the system that the sending of all data of version " + v + " from Data Generators have finished.");
			ByteBuffer buffer = ByteBuffer.allocate(5);
	        buffer.putInt(numberOfMessages.get());
	        buffer.put(v == numOfVersions - 1 ? (byte) 1 : (byte) 0);
	        prevLoadingStartedTime = System.currentTimeMillis();
	        sendToCmdQueue(SystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED, buffer.array());
	        numberOfMessages.set(0);
	        LOGGER.info("Waiting for the system to load data of version " + v);
			versionLoadedMutex.acquire();
		}

        // wait for the data generators to finish their work
        LOGGER.info("Waiting for the data generators to finish their work.");
        waitForDataGenToFinish();
        LOGGER.info("Data generators finished.");
        
        LOGGER.info("Computing system's storage space overhead after data loading");
        ResourceUsageInformation infoAfter = resUsageRequester.getSystemResourceUsage();
        if (infoAfter.getDiskStats() != null) {
        	long systemFinalUsableSpace = infoAfter.getDiskStats().getFsSizeSum();
        	systemStorageSpaceCost = systemFinalUsableSpace - systemInitialUsableSpace;
			LOGGER.info("System's usable space after data loading: " + systemFinalUsableSpace);
			LOGGER.info("System's storage space overhead after data loading: " + systemStorageSpaceCost);
		} else {
			LOGGER.info(infoAfter.toString());
			LOGGER.info("Got null as response.");
		}

        // wait for the task generators to finish their work
        LOGGER.info("Waiting for the task generators to finish their work.");
        waitForTaskGenToFinish();
        LOGGER.info("Task generators finished.");        

        // wait for the system to terminate
        LOGGER.info("Waiting for the system to terminate.");
        waitForSystemToFinish(1000 * 60 * 25);
        LOGGER.info("System terminated.");
    	
    	if (resUsageRequester != null) {
            try {
				resUsageRequester.close();
		        LOGGER.info("Resource Usage Requester closed successfully.");
			} catch (IOException e) {
				LOGGER.error("An error occured while closing SystemResourceUsageRequester");
			}
        }
        
        // pass the number of versions composing the dataset to the environment 
        // variables of the evaluation module
        evalModuleEnvVariables = ArrayUtils.add(evalModuleEnvVariables, VersioningConstants.TOTAL_VERSIONS + "=" + numOfVersions);
        // pass the loading times and the number of triples that have to be loaded to 
        // the environment variables of the evaluation module, so that it can compute
        // the ingestion and applied changes speeds
        for(int version = 0; version < numOfVersions; version++) {
        	evalModuleEnvVariables = ArrayUtils.add(evalModuleEnvVariables, 
        			String.format(VersioningConstants.VERSION_TRIPLES_TO_BE_ADDED, version) + "=" + triplesToBeAdded.get(version));
        	evalModuleEnvVariables = ArrayUtils.add(evalModuleEnvVariables, 
        			String.format(VersioningConstants.VERSION_TRIPLES_TO_BE_DELETED, version) + "=" + triplesToBeDeleted.get(version));
        	evalModuleEnvVariables = ArrayUtils.add(evalModuleEnvVariables, 
        			String.format(VersioningConstants.VERSION_TRIPLES_TO_BE_LOADED, version) + "=" + triplesToBeLoaded.get(version));
        	evalModuleEnvVariables = ArrayUtils.add(evalModuleEnvVariables, 
        			String.format(VersioningConstants.LOADING_TIMES, version) + "=" + loadingTimes[version]);
        }
        
        // pass the storage space cost as an environment variable
        evalModuleEnvVariables = ArrayUtils.add(evalModuleEnvVariables,VersioningConstants.STORAGE_COST_VALUE + "=" + systemStorageSpaceCost);
        
        // create the evaluation module
        createEvaluationModule(EVALUATION_MODULE_CONTAINER_IMAGE, evalModuleEnvVariables);
        
        // wait for the evaluation to finish
        LOGGER.info("Waiting for the evaluation to finish.");
        waitForEvalComponentsToFinish();
        LOGGER.info("Evaluation finished.");

        // Send the resultModul to the platform controller and terminate
        sendResultModel(this.resultModel);
        LOGGER.info("Evaluated results sent to the platform controller.");
	}
}
