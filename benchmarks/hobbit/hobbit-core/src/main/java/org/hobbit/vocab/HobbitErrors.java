/**
 * This file is part of core.
 *
 * core is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * core is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with core.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.hobbit.vocab;

import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * Resources representing Hobbit errors.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class HobbitErrors {

    protected static final String uri = "http://w3id.org/hobbit/error#";

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

    public static final Resource BenchmarkCrashed = resource("BenchmarkCrashed");
    public static final Resource BenchmarkImageMissing = resource("BenchmarkImageMissing");
    public static final Resource BenchmarkCreationError = resource("BenchmarkCreationError");
    public static final Resource ExperimentTookTooMuchTime = resource("ExperimentTookTooMuchTime");
    public static final Resource SystemCrashed = resource("SystemCrashed");
    public static final Resource SystemImageMissing = resource("SystemImageMissing");
    public static final Resource SystemCreationError = resource("SystemCreationError");
    public static final Resource TerminatedByUser = resource("TerminatedByUser");
    public static final Resource UnexpectedError = resource("UnexpectedError");
    public static final Resource ClusterNotHealthy = resource("ClusterNotHealthy");
    public static final Resource UnhandledException = resource("UnhandledException");
    public static final Resource UnspecifiedError = resource("UnspecifiedError");
}
