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

import org.apache.jena.rdf.model.ResourceFactory;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * Vocabulary tests.
 *
 * @author Denis Kuchelev
 *
 */
public class VocabularyTest {

    @Test
    public void testSamples() {
        // test one of members for every vocabulary
        assertEquals("http://purl.org/linked-data/cube#measure",
                DataCube.measure.getURI());
        assertEquals("http://w3id.org/hobbit/vocab#Experiment",
                HOBBIT.Experiment.getURI());
        assertEquals("http://w3id.org/hobbit/challenges#TestChallenge",
                HobbitChallenges.getChallengeURI("TestChallenge"));
        assertEquals("http://w3id.org/hobbit/error#ExperimentTookTooMuchTime",
                HobbitErrors.ExperimentTookTooMuchTime.getURI());
        assertEquals("TestExperiment",
                HobbitExperiments.getExperimentId(ResourceFactory.createResource("http://w3id.org/hobbit/experiments#TestExperiment")));
        assertEquals("http://w3id.org/hobbit/hardware#Node-ExampleNode",
                HobbitHardware.getNodeURI("ExampleNode"));
        assertEquals("http://mex.aksw.org/mex-core#cpu",
                MEXCORE.cpu.getURI());

        assertEquals("http://w3id.org/hobbit/challenges#TestChallenge",
                HobbitChallenges.getChallengeURI("TestChallenge"));
    }

}
