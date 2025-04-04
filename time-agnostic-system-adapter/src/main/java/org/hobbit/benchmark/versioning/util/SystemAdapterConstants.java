package org.hobbit.benchmark.versioning.util;

/**
 * Constants used by the system adapter.
 */
public class SystemAdapterConstants {
    
    /**
     * Command byte used to signal that a bulk data generation has finished.
     */
    public static final byte BULK_LOAD_DATA_GEN_FINISHED = (byte) 1;
    
    /**
     * Command byte used to signal that bulk loading has finished.
     */
    public static final byte BULK_LOADING_DATA_FINISHED = (byte) 2;
    
} 