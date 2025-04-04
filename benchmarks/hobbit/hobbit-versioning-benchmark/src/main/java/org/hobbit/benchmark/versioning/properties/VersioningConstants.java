/**
 * 
 */
package org.hobbit.benchmark.versioning.properties;

/**
 * @author papv
 *
 */
public final class VersioningConstants {
	
	// =============== COMMAND QUEUE CONSTANTS ===============
	
	public static final byte DATA_GEN_VERSION_DATA_SENT = (byte) 301;
		
	// =============== DATA GENERATOR CONSTANTS ===============

	public static final String DATA_GENERATOR_SEED = "data_generator_seed";

	public static final String NUMBER_OF_DATA_GENERATORS= "data_generators_number";
	
	public static final String V0_SIZE_IN_TRIPLES = "v0_size_in_triples";

	public static final String VERSION_INSERTION_RATIO = "version_insertion_ratio";

	public static final String VERSION_DELETION_RATIO = "version_deletion_ratio";
	
	public static final String NUMBER_OF_VERSIONS = "number_of_versions";
					
	public static final String SENT_DATA_FORM = "sent_data_form";

	public static final String ENABLED_QUERY_TYPES = "enabled_query_types";

	// =============== TASK GENERATOR CONSTANTS ===============

	public static final String NUMBER_OF_TASK_GENERATORS= "task_generators_number";
	
	// =============== EVALUATION MODULE CONSTANTS ===============
	
	public static final String INITIAL_VERSION_INGESTION_SPEED = "initial_version_ingestion_speed";
	
	public static final String AVG_APPLIED_CHANGES_PS = "avg_applied_changes_ps";
	
	public static final String STORAGE_COST = "storage_cost";

	public static final String STORAGE_COST_VALUE = "storage_cost_value";

	public static final String QT_1_AVG_EXEC_TIME = "query_type_1_avgerage_execution_time";

	public static final String QT_2_AVG_EXEC_TIME = "query_type_2_avgerage_execution_time";

	public static final String QT_3_AVG_EXEC_TIME = "query_type_3_avgerage_execution_time";

	public static final String QT_4_AVG_EXEC_TIME = "query_type_4_avgerage_execution_time";

	public static final String QT_5_AVG_EXEC_TIME = "query_type_5_avgerage_execution_time";

	public static final String QT_6_AVG_EXEC_TIME = "query_type_6_avgerage_execution_time";

	public static final String QT_7_AVG_EXEC_TIME = "query_type_7_avgerage_execution_time";

	public static final String QT_8_AVG_EXEC_TIME = "query_type_8_avgerage_execution_time";

	public static final String QUERY_FAILURES = "query_failures";
	
	public static final String QUERIES_PER_SECOND = "queries_per_second";
	
	public static final String LOADING_TIMES = "version_%d_loading_time";
	
	public static final String VERSION_TRIPLES_TO_BE_LOADED = "version_%d_triples_to_be_loaded";

	public static final String VERSION_TRIPLES_TO_BE_ADDED = "version_%d_triples_to_be_added";

	public static final String VERSION_TRIPLES_TO_BE_DELETED = "version_%d_triples_to_be_deleted";

	public static final String TOTAL_VERSIONS = "versions_number";
	
	// =============== DBpedia data stats ===============

	public static final int DBPEDIA_VERSIONS = 5;
	
	public static final int DBPEDIA_ADDED_TRIPLES_V0 = 40362;

	public static final int DBPEDIA_DELETED_TRIPLES_V0 = 0;

	public static final int DBPEDIA_ADDED_TRIPLES_V1 = 9315;

	public static final int DBPEDIA_DELETED_TRIPLES_V1 = 14260;

	public static final int DBPEDIA_ADDED_TRIPLES_V2 = 45991;

	public static final int DBPEDIA_DELETED_TRIPLES_V2 = 16888;

	public static final int DBPEDIA_ADDED_TRIPLES_V3 = 16053;

	public static final int DBPEDIA_DELETED_TRIPLES_V3 = 20479;

	public static final int DBPEDIA_ADDED_TRIPLES_V4 = 32884;

	public static final int DBPEDIA_DELETED_TRIPLES_V4 = 21957;
	
	// =============== ontologies data stats ===============
	
	public static final int ONTOLOGIES_TRIPLES = 8134;


}
