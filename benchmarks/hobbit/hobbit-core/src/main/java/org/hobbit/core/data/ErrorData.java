package org.hobbit.core.data;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.hobbit.vocab.HobbitErrors;

/**
 * A simple data structure that represents errors that components can report.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class ErrorData {

    /**
     * ID of the container reporting the error.
     */
    protected String containerName;
    /**
     * IRI of the error type (optional).
     */
    protected String errorType;
    /**
     * A string that can be used as short label of an error (optional, the error
     * type label might be used as default)
     */
    protected String label;
    /**
     * A string that can be used as a short description of an error (optional, the
     * error type description might be used as default).
     */
    protected String description;

    /**
     * @return the containerId
     */
    public String getContainerId() {
        return containerName;
    }

    /**
     * @param containerId the containerId to set
     */
    public void setContainerId(String containerId) {
        this.containerName = containerId;
    }

    /**
     * @return the errorType
     */
    public String getErrorType() {
        return errorType;
    }

    /**
     * @param errorType the errorType to set
     */
    public void setErrorType(String errorType) {
        this.errorType = errorType;
    }

    /**
     * @return the label
     */
    public String getLabel() {
        return label;
    }

    /**
     * @param label the label to set
     */
    public void setLabel(String label) {
        this.label = label;
    }

    /**
     * @return the description
     */
    public String getDescription() {
        return description;
    }

    /**
     * @param description the description to set
     */
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((containerName == null) ? 0 : containerName.hashCode());
        result = prime * result + ((description == null) ? 0 : description.hashCode());
        result = prime * result + ((errorType == null) ? 0 : errorType.hashCode());
        result = prime * result + ((label == null) ? 0 : label.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ErrorData other = (ErrorData) obj;
        if (containerName == null) {
            if (other.containerName != null)
                return false;
        } else if (!containerName.equals(other.containerName))
            return false;
        if (description == null) {
            if (other.description != null)
                return false;
        } else if (!description.equals(other.description))
            return false;
        if (errorType == null) {
            if (other.errorType != null)
                return false;
        } else if (!errorType.equals(other.errorType))
            return false;
        if (label == null) {
            if (other.label != null)
                return false;
        } else if (!label.equals(other.label))
            return false;
        return true;
    }

    /**
     * Creates an instance of a {@link HobbitErrors#UnhandledException} error based
     * on the data of the given exception.
     * 
     * @param e             the exception that should be expressed as error
     * @param containerName the ID of the container that reports the error
     * @return the newly created ErrorData instance or {@code null} if the given
     *         container name is {@code null}.
     */
    public static ErrorData createFromException(Exception e, String containerName) {
        if (containerName == null) {
            return null;
        }
        ErrorData result = new ErrorData();
        result.containerName = containerName;
        result.errorType = HobbitErrors.UnhandledException.getURI();
        result.label = e.getMessage();
        // Get the full stack trace
        StringWriter writer = new StringWriter();
        PrintWriter pwriter = new PrintWriter(writer);
        e.printStackTrace(pwriter);
        pwriter.flush();
        result.description = writer.toString();
        pwriter.close();

        return result;
    }

}
