package org.hobbit.core.data.usage;

public class CpuStats {

    private long totalUsage = 0;

    public CpuStats() {
    }

    /**
     * @param totalUsage the sum of the overall CPU usage to set (in ms)
     */
    public CpuStats(long totalUsage) {
        super();
        this.totalUsage = totalUsage;
    }

    /**
     * @return the sum of the overall CPU usage (in ms)
     */
    public long getTotalUsage() {
        return totalUsage;
    }

    /**
     * @param totalUsage the sum of the overall CPU usage to set (in ms)
     */
    public void setTotalUsage(long totalUsage) {
        this.totalUsage = totalUsage;
    }

    public CpuStats merge(CpuStats cpuStats) {
        this.totalUsage += cpuStats.totalUsage;
        return this;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CpuStats [totalUsage=");
        builder.append(totalUsage);
        builder.append("]");
        return builder.toString();
    }
}
