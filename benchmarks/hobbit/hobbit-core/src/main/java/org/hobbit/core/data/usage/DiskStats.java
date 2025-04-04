package org.hobbit.core.data.usage;

public class DiskStats {

    private long fsSizeSum = 0;

    public DiskStats() {
    }

    /**
     * @param fsSizeSum the sum of the disk usage to set (in bytes)
     */
    public DiskStats(long fsSizeSum) {
        super();
        this.fsSizeSum = fsSizeSum;
    }

    /**
     * @return the sum of the disk usage (in bytes)
     */
    public long getFsSizeSum() {
        return fsSizeSum;
    }

    /**
     * @param fsSizeSum the sum of the disk usage to set (in bytes)
     */
    public void setFsSizeSum(long fsSizeSum) {
        this.fsSizeSum = fsSizeSum;
    }

    public DiskStats merge(DiskStats diskStats) {
        this.fsSizeSum += diskStats.fsSizeSum;
        return this;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DiskStats [fsSizeSum=");
        builder.append(fsSizeSum);
        builder.append("]");
        return builder.toString();
    }

}
