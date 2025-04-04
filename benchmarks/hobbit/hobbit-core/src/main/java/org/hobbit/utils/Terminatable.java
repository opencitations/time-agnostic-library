package org.hobbit.utils;

import java.io.Closeable;

/**
 * Simple interface for something that can be terminated, e.g., a running
 * process. The difference to {@link Closeable} is that a termination does not
 * have to happen while the {@link #terminate()} method is closed.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public interface Terminatable {

    /**
     * Returns a flag whether the object has been asked to terminate. Note that
     * if the method returns {@code true}, it does not have to be terminated.
     * 
     * @return returns {@code true} if it received a request to terminate
     */
    public boolean isTerminated();

    /**
     * Informs the object that it should terminate. Note that there is no
     * guarantee that the termination will happen while this method is running,
     * i.e., this method is not blocking and won't wait for the object to
     * terminate.
     */
    public void terminate();
}
