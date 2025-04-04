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
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;

/**
 * Representation of the Hobbit vocabulary as Java objects.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class HOBBIT {

    protected static final String uri = "http://w3id.org/hobbit/vocab#";

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

    protected static final Property property(String local) {
        return ResourceFactory.createProperty(uri, local);
    }

    // Resources sorted alphabetically
    public static final Resource AnalysisAlgorithm = resource("AnalysisAlgorithm");
    public static final Resource AnalysisResult = resource("AnalysisResult");
    public static final Resource AnalysisResultset = resource("AnalysisResultset");
    public static final Resource API = resource("API");
    public static final Resource AscendingOrder = resource("AscendingOrder");
    public static final Resource Benchmark = resource("Benchmark");
    public static final Resource Challenge = resource("Challenge");
    public static final Resource ChallengeTask = resource("ChallengeTask");
    public static final Resource ConfigurableParameter = resource("ConfigurableParameter");
    public static final Resource DescendingOrder = resource("DescendingOrder");
    public static final Resource Error = resource("Error");
    public static final Resource Experiment = resource("Experiment");
    public static final Resource FeatureParameter = resource("FeatureParameter");
    public static final Resource ForwardedParameter = resource("ForwardedParameter");
    public static final Resource Hardware = resource("Hardware");
    public static final Resource KPI = resource("KPI");
    public static final Resource KPISeq = resource("KPISeq");
    public static final Resource Parameter = resource("Parameter");
    public static final Resource PearsonAlgorithm = resource("PearsonAlgorithm");
    public static final Resource System = resource("System");
    public static final Resource SystemInstance = resource("SystemInstance");

    // Properties sorted alphabetically
    public static final Property algorithm = property("algorithm");
    public static final Property comprises = property("comprises");
    public static final Property closed = property("closed");
    public static final Property dateOfNextExecution = property("dateOfNextExecution");
    public static final Property defaultValue = property("defaultValue");
    public static final Property endTime = property("endTime");
    public static final Property executionDate = property("executionDate");
    public static final Property executionPeriod = property("executionPeriod");
    public static final Property hasAPI = property("hasAPI");
    public static final Property hasCPUTypeCount = property("hasCPUTypeCount");
    public static final Property hasDisks = property("hasDisks");
    public static final Property hasFileSystem = property("hasFileSystem");
    public static final Property hasMemory = property("hasMemory");
    public static final Property hasModel = property("hasModel");
    public static final Property hasNetworkAdapters = property("hasNetworkAdapters");
    public static final Property hasOS = property("hasOS");
    public static final Property hasParameter = property("hasParameter");
    public static final Property hasProcessors = property("hasProcessors");
    public static final Property hasRAM = property("hasRAM");
    public static final Property hobbitPlatformVersion = property("hobbitPlatformVersion");
    public static final Property imageName = property("imageName");
    public static final Property implementsAPI = property("implementsAPI");
    public static final Property instanceOf = property("instanceOf");
    public static final Property involvesBenchmark = property("involvesBenchmark");
    public static final Property involvesKPI = property("involvesKPI");
    public static final Property involvesParameter = property("involvesParameter");
    public static final Property involvesSystemInstance = property("involvesSystemInstance");
    public static final Property isPartOf = property("isPartOf");
    public static final Property isTaskOf = property("isTaskOf");
    public static final Property maxHardware = property("maxHardware");
    public static final Property minHardware = property("minHardware");
    public static final Property measuresKPI = property("measuresKPI");
    public static final Property organizer = property("organizer");
    public static final Property publicationDate = property("publicationDate");
    public static final Property ranking = property("ranking");
    public static final Property rankingKPIs = property("rankingKPIs");
    public static final Property registrationCutoffDate = property("registrationCutoffDate");
    public static final Property relevantForAnalysis = property("relevantForAnalysis");
    public static final Property startTime = property("startTime");
    public static final Property terminatedWithError = property("terminatedWithError");
    public static final Property usesImage = property("usesImage");
    public static final Property version = property("version");
    public static final Property visible = property("visible");
    public static final Property wasCarriedOutOn = property("wasCarriedOutOn");

}
