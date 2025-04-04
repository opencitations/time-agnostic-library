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
 * HOBBIT experiments vocabulary.
 *
 * @author Denis Kuchelev
 *
 */
public class HobbitExperiments {

    protected static final String uri = "http://w3id.org/hobbit/experiments#";

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

    /**
     * returns the experiment resource given its ID
     *
     * @param experimentId the experiment ID
     * @return the experiment resource
     */
    public static Resource getExperiment(String experimentId) {
        return resource(experimentId);
    }

    /**
     * returns the ID of an experiment resource
     *
     * @param experiment the experiment resource
     * @return the ID of experiment
     */
    public static String getExperimentId(Resource experiment) {
        return experiment.getURI().substring(uri.length());
    }

    /**
     * returns the URI of an experiment resource given its ID
     *
     * @param experimentId the experiment ID
     * @return the URI of experiment
     */
    public static String getExperimentURI(String experimentId) {
        return uri + experimentId;
    }

    // Resources sorted alphabetically
    public static final Resource New = resource("New");

}
