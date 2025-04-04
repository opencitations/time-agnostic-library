/**
 * 
 */
package org.hobbit.benchmark.versioning.properties;

/**
 *
 * @author Vassilis Papakonstantinou (papv@ics.forth.gr)
 */
public class RDFUtils {
	
	public static String getFileExtensionFromRdfFormat(String serializationFormat) {
		String formatExt = "nt";
		
		if (serializationFormat.equalsIgnoreCase("TriG")) {
			formatExt = "trig";
		} else if (serializationFormat.equalsIgnoreCase("TriX")) {
			formatExt = "trix";
		} else if (serializationFormat.equalsIgnoreCase("N-Triples")) {
			formatExt = "nt";
		} else if (serializationFormat.equalsIgnoreCase("N-Quads")) {	
			formatExt = "nq";
		} else if (serializationFormat.equalsIgnoreCase("N3")) {
			formatExt = "n3";
		} else if (serializationFormat.equalsIgnoreCase("RDF/XML")) {
			formatExt = "rdf";
		} else if (serializationFormat.equalsIgnoreCase("RDF/JSON")) {
			formatExt = "rj";
		} else if (serializationFormat.equalsIgnoreCase("Turtle")) {
			formatExt = "ttl";
		} else {
			throw new IllegalArgumentException("Warning : unknown serialization format : " + serializationFormat + ", defaulting to N-Triples");
		}		
		
		return formatExt;
	}

}
