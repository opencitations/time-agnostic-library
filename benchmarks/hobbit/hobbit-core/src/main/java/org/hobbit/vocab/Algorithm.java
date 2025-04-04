package org.hobbit.vocab;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * Representation of the Algorithm vocabulary as Java objects.
 *
 * @author Farshad Afshari (farshad.afshari@uni-paderborn.de)
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class Algorithm {
    protected static final String uri = "http://www.w3id.org/dice-research/ontologies/algorithm/2023/06/";

    // Resources sorted alphabetically
    public static final Resource AlgorithmDataRelation = resource("AlgorithmDataRelation");
    public static final Resource AlgorithmExecution = resource("AlgorithmExecution");
    public static final Resource AlgorithmSetup = resource("AlgorithmSetup");
    public static final Resource Parameter = resource("Parameter");
    public static final Resource Result = resource("Result");

    // Properties sorted alphabetically
    public static final Property instanceOf = property("instanceOf");
    public static final Property parameter = property("parameter");
    public static final Property produces = property("produces");
    public static final Property subExecution = property("subExecution");

    /**
     * returns the URI for this schema
     *
     * @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }

    protected static final Resource resource(String local) {
        Resource tmp = ResourceFactory.createResource(uri + local);
        return tmp;

    }

    protected static final Property property(String local) {
        return ResourceFactory.createProperty(uri, local);
    }
}
