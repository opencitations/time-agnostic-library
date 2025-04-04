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
package org.hobbit.utils.rdf;

import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFVisitor;
import org.apache.jena.rdf.model.Resource;

/**
 * A class for serializing URI and literal nodes
 * in the format used for hashing.
 *
 * @author Denis Kuchelev
 *
 */
public class HashingRDFVisitor implements RDFVisitor {

    public static HashingRDFVisitor instance = new HashingRDFVisitor();

    /**
        Method to call when visiting a blank node r with identifier id.
        This implementation does not expect any blank nodes.
        @param r the blank RDF node being visited
        @param id the identifier of that node
        @throws IllegalStateException
    */
    @Override
    public String visitBlank(Resource r, AnonId id) {
        throw new IllegalStateException("This implementation does not expect any blank nodes.");
    }

    /**
        Method to call when visiting a URI node r with the given uri.
        @param r the URI node being visited
        @param uri the URI string of that node
        @return value to be returned from the visit
    */
    @Override
    public String visitURI(Resource r, String uri) {
        return "<" + uri + ">";
    }

    /**
        Method to call when visiting a literal RDF node l.
        @param l the RDF Literal node
        @return a value to be returned from the visit
    */
    @Override
    public String visitLiteral(Literal l) {
        return "\"" + l.getString() + "\"";
    }
}
