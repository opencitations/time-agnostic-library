package org.hobbit.core.data.status;

/**
 * This extension of a {@link QueuedExperiment} represents an experiment that is
 * currently executed and may have additional information, e.g., the status of
 * the execution.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class RunningExperiment extends QueuedExperiment {
    /**
     * The status of the execution of this experiment.
     */
    public String status;
    /**
     * The point in time at which the experiment has been started. If it is 0, the
     * experiment has not been started.
     */
    public long startTimestamp;
    /**
     * The point in time until the experiment will have to be finished.
     */
    public long timestampOfAbortion;

    /**
     * @return the status
     */
    public String getStatus() {
        return status;
    }

    /**
     * @param status
     *            the status to set
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /**
     * @return the startTimestamp
     */
    public long getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * @param startTimestamp
     *            the startTimestamp to set
     */
    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    /**
     * @return the timestampOfAbortion
     */
    public long getTimestampOfAbortion() {
        return timestampOfAbortion;
    }

    /**
     * @param timestampOfAbortion
     *            the timestampOfAbortion to set
     */
    public void setTimestampOfAbortion(long timestampOfAbortion) {
        this.timestampOfAbortion = timestampOfAbortion;
    }
}
