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
 * HOBBIT hardware vocabulary.
 *
 * @author Denis Kuchelev
 *
 */
public class HobbitHardware {

    protected static final String uri = "http://w3id.org/hobbit/hardware#";

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
     * returns the cluster resource given its hash
     *
     * @param clusterHash the cluster hash
     * @return the cluster resource
     */
    public static Resource getCluster(String clusterHash) {
        return resource("Cluster-" + clusterHash);
    }

    /**
     * returns the URI of a cluster resource given its hash
     *
     * @param clusterHash the cluster hash
     * @return the URI of cluster
     */
    public static String getClusterURI(String clusterHash) {
        return uri + "Cluster-" + clusterHash;
    }

    /**
     * returns the node resource given its hash
     *
     * @param nodeHash the node hash
     * @return the node resource
     */
    public static Resource getNode(String nodeHash) {
        return resource("Node-" + nodeHash);
    }

    /**
     * returns the URI of a node resource given its hash
     *
     * @param nodeHash the node hash
     * @return the URI of node
     */
    public static String getNodeURI(String nodeHash) {
        return uri + "Node-" + nodeHash;
    }

}
