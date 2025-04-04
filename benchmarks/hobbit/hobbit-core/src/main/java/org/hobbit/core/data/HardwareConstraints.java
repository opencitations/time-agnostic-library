package org.hobbit.core.data;

public class HardwareConstraints {

    protected String iri;
    protected String label;
    protected long ram;
    protected int cpus;
    /**
     * @return the iri
     */
    public String getIri() {
        return iri;
    }
    /**
     * @param iri the iri to set
     */
    public void setIri(String iri) {
        this.iri = iri;
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
     * @return the ram
     */
    public long getRam() {
        return ram;
    }
    /**
     * @param ram the ram to set
     */
    public void setRam(long ram) {
        this.ram = ram;
    }
    /**
     * @return the cpus
     */
    public int getCpus() {
        return cpus;
    }
    /**
     * @param cpus the cpus to set
     */
    public void setCpus(int cpus) {
        this.cpus = cpus;
    }
}
