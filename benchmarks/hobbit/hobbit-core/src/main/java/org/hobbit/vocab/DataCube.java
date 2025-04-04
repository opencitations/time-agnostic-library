package org.hobbit.vocab;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

public class DataCube {

    protected static final String uri = "http://purl.org/linked-data/cube#";

    /**
     * returns the URI for this schema
     *
     * @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }

    protected static final Resource resource(String local) {
        return ResourceFactory.createResource(uri + local);
    }

    protected static final Property property(String local) {
        return ResourceFactory.createProperty(uri, local);
    }

    // Resources sorted alphabetically
    public static final Resource DataSet = resource("DataSet");

    public static final Resource Attachable = resource("Attachable");
    public static final Resource AttributeProperty = resource("AttributeProperty");
    public static final Resource CodedProperty = resource("CodedProperty");
    public static final Resource ComponentProperty = resource("ComponentProperty");
    public static final Resource ComponentSet = resource("ComponentSet");
    public static final Resource ComponentSpecification = resource("ComponentSpecification");
    public static final Resource DataStructureDefinition = resource("DataStructureDefinition");
    public static final Resource DimensionProperty = resource("DimensionProperty");
    public static final Resource HierarchicalCodeList = resource("HierarchicalCodeList");
    public static final Resource MeasureProperty = resource("MeasureProperty");
    public static final Resource Observation = resource("Observation");
    public static final Resource ObservationGroup = resource("ObservationGroup");
    public static final Resource Slice = resource("Slice");
    public static final Resource SliceKey = resource("SliceKey");

    // Properties sorted alphabetically
    public static final Property attribute = property("attribute");
    public static final Property codeList = property("codeList");
    public static final Property component = property("component");
    public static final Property componentAttachment = property("componentAttachment");
    public static final Property componentProperty = property("componentProperty");
    public static final Property componentRequired = property("componentRequired");
    public static final Property concept = property("concept");
    public static final Property dataSet = property("dataSet");
    public static final Property dimension = property("dimension");
    public static final Property hierarchyRoot = property("hierarchyRoot");
    public static final Property measure = property("measure");
    public static final Property measureDimension = property("measureDimension");
    public static final Property measureType = property("measureType");
    public static final Property observation = property("observation");
    public static final Property observationGroup = property("observationGroup");
    public static final Property order = property("order");
    public static final Property parentChildProperty = property("parentChildProperty");
    public static final Property slice = property("slice");
    public static final Property sliceKey = property("sliceKey");
    public static final Property sliceStructure = property("sliceStructure");
    public static final Property structure = property("structure");

}
