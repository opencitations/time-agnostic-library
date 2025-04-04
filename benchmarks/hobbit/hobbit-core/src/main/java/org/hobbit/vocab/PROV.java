package org.hobbit.vocab;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * Representation of the PROV Ontology (https://www.w3.org/TR/2013/REC-prov-o-20130430/).
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class PROV {

    protected static final String uri = "http://www.w3.org/ns/prov#";

    // Resources sorted alphabetically
    public static final Resource Activity = resource("Activity");
    public static final Resource Agent = resource("Agent");
    public static final Resource Entity = resource("Entity");

    // Properties sorted alphabetically
    public static final Property actedOnBehalfOf = property("actedOnBehalfOf");
    public static final Property endedAtTime = property("endedAtTime");
    public static final Property startedAtTime = property("startedAtTime");
    public static final Property used = property("used");
    public static final Property wasAssociatedWith = property("wasAssociatedWith");
    public static final Property wasAttributedTo = property("wasAttributedTo");
    public static final Property wasDerivedFrom = property("wasDerivedFrom");
    public static final Property wasGeneratedBy = property("wasGeneratedBy");
    public static final Property wasInformedBy = property("wasInformedBy");

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
}
