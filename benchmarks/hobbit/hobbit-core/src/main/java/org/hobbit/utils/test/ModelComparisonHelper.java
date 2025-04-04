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
package org.hobbit.utils.test;

import java.util.HashSet;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;

/**
 * A simple utility class that helps comparing two models.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class ModelComparisonHelper {

    /**
     * Collects statements that can be found in model A but not in model B. If A
     * and B are seen as sets of statements, this method returns the difference
     * A\B.
     *
     * @param modelA
     *            the model that should be fully contained inside model B.
     * @param modelB
     *            the model that should fully contain model A.
     * @return the difference A\B which is empty if A is a subset of B
     */
    public static Set<Statement> getMissingStatements(Model modelA, Model modelB) {
        Set<Statement> statements = new HashSet<>();
        StmtIterator iterator = modelA.listStatements();
        Statement s;
        while (iterator.hasNext()) {
            s = iterator.next();
            if (!modelContainsStatement(modelB, s)) {
                statements.add(s);
            }
        }
        return statements;
    }

    /**
     * Checks whether the given statement can be found in the given model. If
     * the given statement contains blank nodes (= Anon nodes) they are replaced
     * by variables.
     *
     * @param model
     *            the model that might contain the given statement
     * @param s
     *            the statement which could be contained in the given model
     * @return <code>true</code> if the statement can be found in the model,
     *         <code>false</code> otherwise
     */
    public static boolean modelContainsStatement(Model model, Statement s) {
        Resource subject = s.getSubject();
        RDFNode object = s.getObject();
        if (subject.isAnon()) {
            if (object.isAnon()) {
                return model.contains(null, s.getPredicate(), (RDFNode) null);
            } else {
                return model.contains(null, s.getPredicate(), object);
            }
        } else {
            if (object.isAnon()) {
                return model.contains(subject, s.getPredicate(), (RDFNode) null);
            } else {
                return model.contains(subject, s.getPredicate(), object);
            }
        }
    }

}
