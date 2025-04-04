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

import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * HOBBIT namespace for error instances.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class HobbitErrorInstances {

    protected static final String uri = "http://w3id.org/hobbit/error-instance#";

    /**
     * returns the URI for this schema
     *
     * @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }

    protected static final Resource resource(String local) {
        return ResourceFactory.createResource(getErrorInstanceUri(local));
    }

    /**
     * returns the error-instance resource given its ID
     *
     * @param instanceId the ID of the error instance
     * @return the error instance resource
     */
    public static Resource getErrorInstance(String instanceId) {
        return resource(instanceId);
    }

    /**
     * returns the URI of an error instance given its ID
     *
     * @param instanceId the ID of the error instance
     * @return the error instance URI
     */
    public static String getErrorInstanceUri(String instanceId) {
        return uri + instanceId;
    }

}
