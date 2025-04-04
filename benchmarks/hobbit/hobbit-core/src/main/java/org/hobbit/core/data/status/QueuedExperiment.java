package org.hobbit.core.data.status;

/**
 * Instances of this class represent an experiment that is waiting in the queue
 * to be executed.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class QueuedExperiment {

    /**
     * The ID of the experiment.
     */
    public String experimentId;
    /**
     * The URI of the benchmark.
     */
    public String benchmarkUri;
    /**
     * The name of the benchmark.
     */
    public String benchmarkName;
    /**
     * The URI of the system.
     */
    public String systemUri;
    /**
     * The name of the system.
     */
    public String systemName;
    /**
     * The URI of the challenge this experiment is part of (or {@code null} if such
     * a challenge does not exist).
     */
    public String challengeUri;
    /**
     * The URI of the challenge task this experiment is part of (or {@code null} if
     * such a challenge does not exist).
     */
    public String challengeTaskUri;
    /**
     * Date of execution in milliseconds.
     */
    public long dateOfExecution = 0;
    /**
     * A flag indicating whether the experiment can be canceled by the user who
     * requested this information.
     */
    public boolean canBeCanceled = false;

    /**
     * @return the experimentId
     */
    public String getExperimentId() {
        return experimentId;
    }

    /**
     * @param experimentId
     *            the experimentId to set
     */
    public void setExperimentId(String experimentId) {
        this.experimentId = experimentId;
    }

    /**
     * @return the benchmarkUri
     */
    public String getBenchmarkUri() {
        return benchmarkUri;
    }

    /**
     * @param benchmarkUri
     *            the benchmarkUri to set
     */
    public void setBenchmarkUri(String benchmarkUri) {
        this.benchmarkUri = benchmarkUri;
    }

    /**
     * @return the benchmarkName
     */
    public String getBenchmarkName() {
        return benchmarkName;
    }

    /**
     * @param benchmarkName
     *            the benchmarkName to set
     */
    public void setBenchmarkName(String benchmarkName) {
        this.benchmarkName = benchmarkName;
    }

    /**
     * @return the systemUri
     */
    public String getSystemUri() {
        return systemUri;
    }

    /**
     * @param systemUri
     *            the systemUri to set
     */
    public void setSystemUri(String systemUri) {
        this.systemUri = systemUri;
    }

    /**
     * @return the systemName
     */
    public String getSystemName() {
        return systemName;
    }

    /**
     * @param systemName
     *            the systemName to set
     */
    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    /**
     * @return the challengeUri
     */
    public String getChallengeUri() {
        return challengeUri;
    }

    /**
     * @param challengeUri
     *            the challengeUri to set
     */
    public void setChallengeUri(String challengeUri) {
        this.challengeUri = challengeUri;
    }

    /**
     * @return the challengeTaskUri
     */
    public String getChallengeTaskUri() {
        return challengeTaskUri;
    }

    /**
     * @param challengeTaskUri
     *            the challengeTaskUri to set
     */
    public void setChallengeTaskUri(String challengeTaskUri) {
        this.challengeTaskUri = challengeTaskUri;
    }

    /**
     * @return the canBeCanceled
     */
    public boolean isCanBeCanceled() {
        return canBeCanceled;
    }

    /**
     * @param canBeCanceled
     *            the canBeCanceled to set
     */
    public void setCanBeCanceled(boolean canBeCanceled) {
        this.canBeCanceled = canBeCanceled;
    }

}
