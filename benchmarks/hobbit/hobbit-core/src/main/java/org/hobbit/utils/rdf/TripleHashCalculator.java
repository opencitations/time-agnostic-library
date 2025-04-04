package org.hobbit.utils.rdf;

import java.util.stream.Collectors;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.jena.ext.com.google.common.collect.Streams;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;


/**
 * A class for computing a SHA1 hash of
 * sorted and serialized resource properties.
 *
 */
public class TripleHashCalculator {

    /**
     * Replacement URI for the resource.
     * Currently has no effect on actual hash computation.
     */
    public static String HASH_SELF_URI = "hashedRDF.v1:self";

    /**
     * @param statements
     * @return the computed hash.
     */
    public static String calculateHash(StmtIterator statements) {
        String s = Streams.stream(statements)
                .map(TripleHashCalculator::serializeStatement)
                .sorted()
                .collect(Collectors.joining());

        return DigestUtils.sha1Hex(s);
    }

    private static String serializeStatement(Statement stmt) {
        StringBuilder s = new StringBuilder();
        s.append(stmt.getPredicate().visitWith(HashingRDFVisitor.instance));
        s.append(" ");
        s.append(stmt.getObject().visitWith(HashingRDFVisitor.instance));
        s.append("\n");
        return s.toString();
    }

}
