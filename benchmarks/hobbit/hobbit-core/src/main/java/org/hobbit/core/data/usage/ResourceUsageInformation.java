package org.hobbit.core.data.usage;

public class ResourceUsageInformation {

    private CpuStats cpuStats;
    private DiskStats diskStats;
    private MemoryStats memoryStats;
    
    public ResourceUsageInformation() {
    }
    
    public ResourceUsageInformation(CpuStats cpuStats, DiskStats diskStats, MemoryStats memoryStats) {
        this.diskStats = diskStats;
        this.memoryStats = memoryStats;
        this.cpuStats = cpuStats;
    }

    /**
     * @return the cpuStats
     */
    public CpuStats getCpuStats() {
        return cpuStats;
    }

    /**
     * @param cpuStats the cpuStats to set
     */
    public void setCpuStats(CpuStats cpuStats) {
        this.cpuStats = cpuStats;
    }

    /**
     * @return the diskStats
     */
    public DiskStats getDiskStats() {
        return diskStats;
    }

    /**
     * @param diskStats the diskStats to set
     */
    public void setDiskStats(DiskStats diskStats) {
        this.diskStats = diskStats;
    }

    /**
     * @return the memoryStats
     */
    public MemoryStats getMemoryStats() {
        return memoryStats;
    }

    /**
     * @param memoryStats the memoryStats to set
     */
    public void setMemoryStats(MemoryStats memoryStats) {
        this.memoryStats = memoryStats;
    }
    
    public ResourceUsageInformation merge(ResourceUsageInformation other) {
        if(this.diskStats != null) {
            if(other.diskStats != null) {
                this.diskStats = this.diskStats.merge(other.diskStats);
            }
        } else if(other.diskStats != null) {
            this.diskStats = other.diskStats;
        }
        if(this.memoryStats != null) {
            if(other.memoryStats != null) {
                this.memoryStats = this.memoryStats.merge(other.memoryStats);
            }
        } else if(other.memoryStats != null) {
            this.memoryStats = other.memoryStats;
        }
        if(this.cpuStats != null) {
            if(other.cpuStats != null) {
                this.cpuStats = this.cpuStats.merge(other.cpuStats);
            }
        } else if(other.cpuStats != null) {
            this.cpuStats = other.cpuStats;
        }
        return this;
    }
    
    public static ResourceUsageInformation staticMerge(ResourceUsageInformation o1, ResourceUsageInformation o2) {
        if(o1 == null) {
            return o2;
        }
        if(o2 == null) {
            return o1;
        }
        return o1.merge(o2);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ResourceUsageInformation [");
        if (cpuStats != null) {
            builder.append("cpuStats=");
            builder.append(cpuStats);
            builder.append(", ");
        }
        if (diskStats != null) {
            builder.append("diskStats=");
            builder.append(diskStats);
            builder.append(", ");
        }
        if (memoryStats != null) {
            builder.append("memoryStats=");
            builder.append(memoryStats);
        }
        builder.append("]");
        return builder.toString();
    }
}
