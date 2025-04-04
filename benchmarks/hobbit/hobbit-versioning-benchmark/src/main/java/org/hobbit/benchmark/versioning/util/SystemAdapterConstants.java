/**
 * 
 */
package org.hobbit.benchmark.versioning.util;

/**
 * @author papv
 *
 */
public final class SystemAdapterConstants {
	
	/**
     * The signal sent by the benchmarked system to indicate that it
     * has finished with a phase of bulk loading.
     */
	public static final byte  BULK_LOADING_DATA_FINISHED = (byte) 150;
	/**
     * The signal sent by the benchmark controller to indicate that all
     * data has successfully sent by the data generators
     */
    public static final byte  BULK_LOAD_DATA_GEN_FINISHED = (byte) 151;
}
