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

import java.util.stream.Stream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.rdf.model.impl.StmtIteratorImpl;
import org.hobbit.utils.rdf.TripleHashCalculator;

/**
 * HOBBIT analysis vocabulary.
 *
 * @author Denis Kuchelev
 *
 */
public class HobbitAnalysis {

    protected static final String uri = "http://w3id.org/hobbit/analysis#";

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
     * returns the resultset resource given its benchmark and system instance
     *
     * @param benchmark the benchmark this resultset involves
     * @param systemInstance the system instance this resultset involves
     * @return the resultset resource
     */
    public static Resource getResultset(Resource benchmark, Resource systemInstance) {
        Model dummyModel = ModelFactory.createDefaultModel();
        Resource dummyRes = dummyModel.createResource(TripleHashCalculator.HASH_SELF_URI);
        String hash = TripleHashCalculator.calculateHash(new StmtIteratorImpl(Stream.of(
            (Statement) new StatementImpl(dummyRes, HOBBIT.involvesBenchmark, benchmark),
            (Statement) new StatementImpl(dummyRes, HOBBIT.involvesSystemInstance, systemInstance)
        ).iterator()));

        return resource("Resultset-" + hash);
    }

}
