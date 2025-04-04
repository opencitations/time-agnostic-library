package org.hobbit.core.rabbit;

import java.io.Closeable;

import org.hobbit.core.data.RabbitQueue;

/**
 * An interface for a class that receives data and offers methods to close the
 * receiving when it is finished and to check the receiver for errors that might
 * have been encountered.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public interface DataReceiver extends Closeable {

    /**
     * Returns the number of errors that have been encountered while receiving
     * data. If this number is not 0 the receiver can not guarantee that all
     * data has been received correctly.
     * 
     * @return the number of errors encountered during the receiving of data
     */
    public int getErrorCount();

    /**
     * This method waits for the {@link DataReceiver} to finish its work and
     * closes the incoming queue as well as the internal thread pool after that.
     * For making sure that all data is received this method should be preferred
     * compared to the {@link #close()} method which might close the receiver in
     * a rude way.
     */
    public void closeWhenFinished();

    public void increaseErrorCount();
    
    public DataHandler getDataHandler();
    
    public RabbitQueue getQueue();
}