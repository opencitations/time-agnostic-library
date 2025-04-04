package org.hobbit.core.data;

/**
 * This Exception is a wrapper of an exception that has already been reported on
 * the command queue and, hence, doesn't have to be reported again.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class ReportedException extends Exception {

    private static final long serialVersionUID = 2L;

    public ReportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public ReportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public ReportedException(Throwable cause) {
        super(cause);
    }

}
