package org.hobbit.benchmark.versioning.components;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.query.ResultSetRewindable;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.resultset.ResultSetCompare;
import org.apache.jena.vocabulary.RDF;
import org.hobbit.benchmark.versioning.IngestionStatistics;
import org.hobbit.benchmark.versioning.QueryTypeStatistics;
import org.hobbit.benchmark.versioning.properties.VersioningConstants;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractEvaluationModule;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.vocab.HOBBIT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersioningEvaluationModule extends AbstractEvaluationModule {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningEvaluationModule.class);
	
    private Model finalModel = ModelFactory.createDefaultModel();
    
    private Property INITIAL_VERSION_INGESTION_SPEED = null;
    private Property AVG_APPLIED_CHANGES_PS = null;
    private Property STORAGE_COST = null;
    private Property QT_1_AVG_EXEC_TIME = null;
    private Property QT_2_AVG_EXEC_TIME = null;
    private Property QT_3_AVG_EXEC_TIME = null;
    private Property QT_4_AVG_EXEC_TIME = null;
    private Property QT_5_AVG_EXEC_TIME = null;
    private Property QT_6_AVG_EXEC_TIME = null;
    private Property QT_7_AVG_EXEC_TIME = null;
    private Property QT_8_AVG_EXEC_TIME = null;
    private Property QUERY_FAILURES = null;
    private Property QUERIES_PER_SECOND = null;
    
    private IngestionStatistics is = new IngestionStatistics();
    private QueryTypeStatistics qts1 = new QueryTypeStatistics(1);
    private QueryTypeStatistics qts2 = new QueryTypeStatistics(2);
    private QueryTypeStatistics qts3 = new QueryTypeStatistics(3);
    private QueryTypeStatistics qts4 = new QueryTypeStatistics(4);
    private QueryTypeStatistics qts5 = new QueryTypeStatistics(5);
    private QueryTypeStatistics qts6 = new QueryTypeStatistics(6);
    private QueryTypeStatistics qts7 = new QueryTypeStatistics(7);
    private QueryTypeStatistics qts8 = new QueryTypeStatistics(8);
    
	private float storageCost = 0;
	private int queryFailures = 0;
	private int numberOfVersions = 0;
	private float qps = 0;

	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing Evaluation Module...");
        // Always init the super class first!
        super.init();
        
        Map<String, String> env = System.getenv();
        
        // get the loading times and the triples that have to be loaded for each version
        // and report them in order to be ready for computing the ingestion and 
        // applied changes speeds
        numberOfVersions = Integer.parseInt(env.get(VersioningConstants.TOTAL_VERSIONS));
        for(int version=0; version<numberOfVersions; version++) {
        	long loadingTime = Long.parseLong(env.get(String.format(VersioningConstants.LOADING_TIMES, version)));
        	int triplesToBeAdded = Integer.parseInt(env.get(String.format(VersioningConstants.VERSION_TRIPLES_TO_BE_ADDED, version)));
        	int triplesToBeDeleted = Integer.parseInt(env.get(String.format(VersioningConstants.VERSION_TRIPLES_TO_BE_DELETED, version)));
        	int triplesToBeLoaded = Integer.parseInt(env.get(String.format(VersioningConstants.VERSION_TRIPLES_TO_BE_LOADED, version)));
        	LOGGER.info("version " + version + "(+" + triplesToBeAdded + ", -" + triplesToBeDeleted + ", total=" + triplesToBeLoaded + ") loaded in " + loadingTime + " ms.");
        	is.reportSuccess(version, triplesToBeAdded, triplesToBeDeleted, triplesToBeLoaded, loadingTime);
        }
        
        storageCost = Long.parseLong(env.get(String.format(VersioningConstants.STORAGE_COST_VALUE))) / (1024f * 1024f);
        
        INITIAL_VERSION_INGESTION_SPEED = initFinalModelFromEnv(env, VersioningConstants.INITIAL_VERSION_INGESTION_SPEED);
        AVG_APPLIED_CHANGES_PS = initFinalModelFromEnv(env, VersioningConstants.AVG_APPLIED_CHANGES_PS);
        STORAGE_COST = initFinalModelFromEnv(env, VersioningConstants.STORAGE_COST);
        QT_1_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_1_AVG_EXEC_TIME);        
        QT_2_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_2_AVG_EXEC_TIME);
        QT_3_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_3_AVG_EXEC_TIME);
        QT_4_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_4_AVG_EXEC_TIME);
        QT_5_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_5_AVG_EXEC_TIME);
        QT_6_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_6_AVG_EXEC_TIME);
        QT_7_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_7_AVG_EXEC_TIME);
        QT_8_AVG_EXEC_TIME = initFinalModelFromEnv(env, VersioningConstants.QT_8_AVG_EXEC_TIME);
        QUERY_FAILURES = initFinalModelFromEnv(env, VersioningConstants.QUERY_FAILURES);        
        QUERIES_PER_SECOND = initFinalModelFromEnv(env, VersioningConstants.QUERIES_PER_SECOND);       

		LOGGER.info("Evaluation Module initialized successfully.");
    }
	
	/**
     * Initialize evaluation module parameters from environment variables
     * 
     * @param env		a map of all available environment variables
     * @param parameter	the property that we want to get
     */
	private Property initFinalModelFromEnv(Map<String, String> env, String parameter) {
		if (!env.containsKey(parameter)) {
			LOGGER.error(
					"Environment variable \"" + parameter + "\" is not set. Aborting.");
            throw new IllegalArgumentException(
            		"Environment variable \"" + parameter + "\" is not set. Aborting.");
        }
		Property property =  finalModel.createProperty(env.get(parameter));
		return property;
	}

	@Override
	protected void evaluateResponse(byte[] expectedData, byte[] receivedData, long taskSentTimestamp,
			long responseReceivedTimestamp) throws Exception {
		
		ByteBuffer expectedBuffer = ByteBuffer.wrap(expectedData);
		
		// get the query type
		int queryType = expectedBuffer.getInt();
		int querySubType = expectedBuffer.getInt();
		int querySustitutionParam = expectedBuffer.getInt();

		
		// get the expected results
		byte expectedDataBytes[] = RabbitMQUtils.readByteArray(expectedBuffer);
		InputStream inExpected = new ByteArrayInputStream(expectedDataBytes);
		ResultSetRewindable expected = ResultSetFactory.makeRewindable(ResultSetFactory.fromJSON(inExpected));
				
		// get the returned results
		InputStream inReceived = new ByteArrayInputStream(receivedData);
		ResultSetRewindable received = ResultSetFactory.makeRewindable(ResultSetFactory.fromJSON(inReceived));

		// if the result size is larger than 50K triples only compare result sizes.
		// if not compare the results themselves
		boolean resultSetsEqual = (expected.size() == received.size() && expected.size() > 50000) ? expected.size() == received.size() : ResultSetCompare.equalsByValue(expected, received);
		
		switch (queryType) {
			case 1:
				if(resultSetsEqual) {
					qts1.reportSuccess(responseReceivedTimestamp - taskSentTimestamp); 
				} else {
					qts1.reportFailure();
				}
				break;
			case 2:	
				if(resultSetsEqual) { 
					qts2.reportSuccess(responseReceivedTimestamp - taskSentTimestamp); 
				} else { 
					qts2.reportFailure();
				}
				break;
			case 3:	
				if(resultSetsEqual) {
					qts3.reportSuccess(responseReceivedTimestamp - taskSentTimestamp); 
				} else { 
					qts3.reportFailure();
				}
				break;
			case 4:	
				if(resultSetsEqual) { 
					qts4.reportSuccess(responseReceivedTimestamp - taskSentTimestamp); 
				} else { 
					qts4.reportFailure();
				}
				break;
			case 5:	
				if(resultSetsEqual) {  
					qts5.reportSuccess(responseReceivedTimestamp - taskSentTimestamp); 
				} else { 
					qts5.reportFailure();
				}
				break;
			case 6:				
				if(resultSetsEqual) {  
					qts6.reportSuccess(responseReceivedTimestamp - taskSentTimestamp); 
				} else { 
					qts6.reportFailure();
				}
				break;
			case 7:	
				if(resultSetsEqual) {  
					qts7.reportSuccess(responseReceivedTimestamp - taskSentTimestamp); 
				} else { 
					qts7.reportFailure(); 
				}
				break;
			case 8:	
				if(resultSetsEqual) {  
					qts8.reportSuccess(responseReceivedTimestamp - taskSentTimestamp); 
				} else { 
					qts8.reportFailure(); 
				}
				break;
		}
		LOGGER.info((resultSetsEqual ? "[SUCCESS]" : "[FAIL]") + " - Task type: " + queryType + "." + querySubType + "." + querySustitutionParam + " executed in " + (responseReceivedTimestamp - taskSentTimestamp) + " ms and returned " + received.size() + "/" + expected.size() + " results.");
	}
	
	private void computeTotalFailures() {
		queryFailures += qts1.getFailuresCount();
		queryFailures += qts2.getFailuresCount();
		queryFailures += qts3.getFailuresCount();
		queryFailures += qts4.getFailuresCount();
		queryFailures += qts5.getFailuresCount();
		queryFailures += qts6.getFailuresCount();
		queryFailures += qts7.getFailuresCount();
		queryFailures += qts8.getFailuresCount();
	}

	private void computeQPS() {
		float totalQueriesExecutionTime = 0;
		totalQueriesExecutionTime += qts1.getTotalExecutionTimeMs();
		totalQueriesExecutionTime += qts2.getTotalExecutionTimeMs();
		totalQueriesExecutionTime += qts3.getTotalExecutionTimeMs();
		totalQueriesExecutionTime += qts4.getTotalExecutionTimeMs();
		totalQueriesExecutionTime += qts5.getTotalExecutionTimeMs();
		totalQueriesExecutionTime += qts6.getTotalExecutionTimeMs();
		totalQueriesExecutionTime += qts7.getTotalExecutionTimeMs();
		totalQueriesExecutionTime += qts8.getTotalExecutionTimeMs();
		
		long totalQueriesCount = 0;
		totalQueriesCount += qts1.getRunsCount();
		totalQueriesCount += qts2.getRunsCount();
		totalQueriesCount += qts3.getRunsCount();
		totalQueriesCount += qts4.getRunsCount();
		totalQueriesCount += qts5.getRunsCount();
		totalQueriesCount += qts6.getRunsCount();
		totalQueriesCount += qts7.getRunsCount();
		totalQueriesCount += qts8.getRunsCount();
		
		float qps_curr = (float) totalQueriesCount / (totalQueriesExecutionTime / 1000);
		qps = Double.isNaN(qps_curr) ? 0 : qps_curr;
	}
	
	@Override
	protected Model summarizeEvaluation() throws Exception {
		LOGGER.info("Summarizing evaluation...");
		
		if (experimentUri == null) {
            Map<String, String> env = System.getenv();
            this.experimentUri = env.get(Constants.HOBBIT_EXPERIMENT_URI_KEY);
        }
		
		// write the summarized results into a Jena model and send it to the benchmark controller.
		Resource experimentResource = finalModel.getResource(experimentUri);
		finalModel.add(experimentResource , RDF.type, HOBBIT.Experiment);
		
		Literal initialVersionIngestionSpeedLiteral = finalModel.createTypedLiteral((is.getFailuresCount() == 0) ? is.getInitialVersionIngestionSpeed() : 0f, XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, INITIAL_VERSION_INGESTION_SPEED, initialVersionIngestionSpeedLiteral);
		LOGGER.info("INITIAL_VERSION_INGESTION_SPEED: " + is.getInitialVersionIngestionSpeed());

		Literal avgAppliedChangesPSLiteral = finalModel.createTypedLiteral(is.getAvgChangesPS(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, AVG_APPLIED_CHANGES_PS, avgAppliedChangesPSLiteral);
        LOGGER.info("AVG_APPLIED_CHANGES_PS: " + is.getAvgChangesPS());
        
        Literal storageCostLiteral = finalModel.createTypedLiteral(storageCost, XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, STORAGE_COST, storageCostLiteral);
        LOGGER.info("STORAGE_COST: " + storageCost);
        
        Literal queryType1AvgExecTimeLiteral = finalModel.createTypedLiteral(qts1.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_1_AVG_EXEC_TIME, queryType1AvgExecTimeLiteral);
        LOGGER.info("QT_1_AVG_EXEC_TIME: " + qts1.getAvgExecutionTimeMs());
        
        Literal queryType2AvgExecTimeLiteral = finalModel.createTypedLiteral(qts2.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_2_AVG_EXEC_TIME, queryType2AvgExecTimeLiteral);
        LOGGER.info("QT_2_AVG_EXEC_TIME: " + qts2.getAvgExecutionTimeMs());

        Literal queryType3AvgExecTimeLiteral = finalModel.createTypedLiteral(qts3.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_3_AVG_EXEC_TIME, queryType3AvgExecTimeLiteral);
        LOGGER.info("QT_3_AVG_EXEC_TIME: " + qts3.getAvgExecutionTimeMs());

        Literal queryType4AvgExecTimeLiteral = finalModel.createTypedLiteral(qts4.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_4_AVG_EXEC_TIME, queryType4AvgExecTimeLiteral);
        LOGGER.info("QT_4_AVG_EXEC_TIME: " + qts4.getAvgExecutionTimeMs());
        
        Literal queryType5AvgExecTimeLiteral = finalModel.createTypedLiteral(qts5.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_5_AVG_EXEC_TIME, queryType5AvgExecTimeLiteral);
        LOGGER.info("QT_5_AVG_EXEC_TIME: " + qts5.getAvgExecutionTimeMs());
        
        Literal queryType6AvgExecTimeLiteral = finalModel.createTypedLiteral(qts6.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_6_AVG_EXEC_TIME, queryType6AvgExecTimeLiteral);
        LOGGER.info("QT_6_AVG_EXEC_TIME: " + qts6.getAvgExecutionTimeMs());
        
        Literal queryType7AvgExecTimeLiteral = finalModel.createTypedLiteral(qts7.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_7_AVG_EXEC_TIME, queryType7AvgExecTimeLiteral);
        LOGGER.info("QT_7_AVG_EXEC_TIME: " + qts7.getAvgExecutionTimeMs());
        
        Literal queryType8AvgExecTimeLiteral = finalModel.createTypedLiteral(qts8.getAvgExecutionTimeMs(), XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QT_8_AVG_EXEC_TIME, queryType8AvgExecTimeLiteral);
        LOGGER.info("QT_8_AVG_EXEC_TIME: " + qts8.getAvgExecutionTimeMs());
        
		// Compute the number of queries that failed to be executed.	
		computeTotalFailures();
        Literal queryFailuresLiteral = finalModel.createTypedLiteral(queryFailures, XSDDatatype.XSDunsignedInt);
        finalModel.add(experimentResource, QUERY_FAILURES, queryFailuresLiteral);
        LOGGER.info("QUERY_FAILURES: " + queryFailures);
        
		// Compute the queries that successfully executed per second.
		computeQPS();
        Literal queriesPerSecondLiteral = finalModel.createTypedLiteral(qps, XSDDatatype.XSDfloat);
        finalModel.add(experimentResource, QUERIES_PER_SECOND, queriesPerSecondLiteral);
        LOGGER.info("QUERIES_PER_SECOND: " + qps);

        return finalModel;
	}
}