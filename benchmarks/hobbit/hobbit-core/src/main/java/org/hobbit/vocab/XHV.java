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
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * Representation of the Creative Commons vocabulary as Java objects.
 */
public class XHV {

    protected static final String uri = "http://www.w3.org/1999/xhtml/vocab#";

    /**
     * returns the URI for this schema
     *
     * @return the URI for this schema
     */
    public static String getURI() {
        return uri;
    }

    protected static final Property property(String local) {
        return ResourceFactory.createProperty(uri, local);
    }

    // Properties sorted alphabetically
    public static final Property icon = property("icon");

}
