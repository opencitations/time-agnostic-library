package org.hobbit.utils;

/**
 * An interface that merges the interfaces {@link Runnable} and
 * {@link Terminatable}.
 * 
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public interface TerminatableRunnable extends Runnable, Terminatable {

}
