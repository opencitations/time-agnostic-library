package org.hobbit.core.data.usage;

public class MemoryStats {

    private long usageSum = 0;

    public MemoryStats() {
    }

    /**
     * @param usageSum the sum of the memory usage to set (in bytes)
     */
    public MemoryStats(long usageSum) {
        this.usageSum = usageSum;
    }

    /**
     * @return the sum of the memory usage (in bytes)
     */
    public long getUsageSum() {
        return usageSum;
    }

    /**
     * @param usageSum the sum of the memory usage to set (in bytes)
     */
    public void setUsageSum(long usageSum) {
        this.usageSum = usageSum;
    }

    public MemoryStats merge(MemoryStats memoryStats) {
        this.usageSum += memoryStats.usageSum;
        return this;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("MemoryStats [usageSum=");
        builder.append(usageSum);
        builder.append("]");
        return builder.toString();
    }
}
