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

import static org.junit.Assert.assertEquals;

import java.util.stream.Stream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.StatementImpl;
import org.apache.jena.rdf.model.impl.StmtIteratorImpl;
import org.junit.Before;
import org.junit.Test;

public class HashResourceTest {

    private Model m;

    @Before
    public void setUp() throws Exception {
        m = ModelFactory.createDefaultModel();
    }

    @Test
    public void testHashResourceWithLiteral() {
        String hash = TripleHashCalculator.calculateHash(new StmtIteratorImpl(Stream.of(
            (Statement) new StatementImpl(
                m.createResource(TripleHashCalculator.HASH_SELF_URI),
                m.createProperty("http://example.org/exampleProperty"),
                m.createLiteral("exampleLiteralValue"))
        ).iterator()));

        assertEquals("Hash of resource with a single literal value",
                "7d382754e28fd2ee70f6dbdb5feb0a4caa9d4dbf", hash);
    }

    @Test
    public void testHashResourceWithURI() {
        String hash = TripleHashCalculator.calculateHash(new StmtIteratorImpl(Stream.of(
            (Statement) new StatementImpl(
                m.createResource(TripleHashCalculator.HASH_SELF_URI),
                m.createProperty("http://example.org/exampleProperty"),
                m.createResource("http://example.org/ExampleResource"))
        ).iterator()));

        assertEquals("Hash of resource with a single URI value",
                "76129f18357ee830f85f21565c8efadb7b0a567e", hash);
    }

    @Test
    public void testHashResourceWithSeveralProperties() {
        String hash = TripleHashCalculator.calculateHash(new StmtIteratorImpl(Stream.of(
            (Statement) new StatementImpl(
                m.createResource(TripleHashCalculator.HASH_SELF_URI),
                m.createProperty("http://example.org/exampleProperty"),
                m.createLiteral("exampleValue2")),
            new StatementImpl(
                m.createResource(TripleHashCalculator.HASH_SELF_URI),
                m.createProperty("http://example.org/exampleProperty"),
                m.createLiteral("exampleValue1"))
        ).iterator()));

        assertEquals("Hash of resource with several properties",
                "8b775c924725d8dc8d2e2fc6d1dd74d8880248a7", hash);
    }

}
