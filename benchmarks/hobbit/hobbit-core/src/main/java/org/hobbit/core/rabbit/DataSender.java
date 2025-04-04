package org.hobbit.core.rabbit;

import java.io.Closeable;
import java.io.IOException;

/**
 * An interface of a class sending data to a queue.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public interface DataSender extends Closeable {

    /**
     * Send the given data to the queue.
     * 
     * @param data
     *            the data that should be sent
     * @throws IOException
     *             if an exception occurs during the communication with the
     *             queue
     */
    public void sendData(byte[] data) throws IOException;

    /**
     * A blocking method that closes the sender when its work is done, i.e., all
     * messages have been consumed by receivers from the queue.
     */
    public void closeWhenFinished();

}