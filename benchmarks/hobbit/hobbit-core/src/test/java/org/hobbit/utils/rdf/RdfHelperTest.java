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

import java.io.StringReader;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TimeZone;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.vocabulary.RDFS;
import org.junit.Assert;
import org.junit.Test;

public class RdfHelperTest {

    private static final double DELTA = 1e-15;

    @Test
    public void testGetLabel() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), RDFS.label, "Label 1");
        model.add(model.getResource("http://example.org/example1"), RDFS.comment, "Comment 1");
        model.add(model.getResource("http://example.org/example2"), RDFS.label, "Second label");
        model.add(model.getResource("http://example.org/example3"), RDFS.comment, "Comment for resource 3");

        Assert.assertEquals("Label 1", RdfHelper.getLabel(model, model.getResource("http://example.org/example1")));
        Assert.assertEquals("Second label",
                RdfHelper.getLabel(model, model.getResource("http://example.org/example2")));

        Assert.assertTrue((new HashSet<String>(Arrays.asList("Label 1", "Second label"))
                .contains(RdfHelper.getLabel(model, null))));

        Assert.assertNull(RdfHelper.getLabel(model, model.getResource("http://example.org/example3")));
        Assert.assertNull(RdfHelper.getLabel(model, model.getResource("http://example.org/example4")));
        Assert.assertNull(RdfHelper.getLabel(null, model.getResource("http://example.org/example1")));
    }

    @Test
    public void testGetDescription() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), RDFS.label, "Label 1");
        model.add(model.getResource("http://example.org/example1"), RDFS.comment, "Comment 1");
        model.add(model.getResource("http://example.org/example2"), RDFS.label, "Second label");
        model.add(model.getResource("http://example.org/example3"), RDFS.comment, "Comment for resource 3");

        Assert.assertEquals("Comment 1",
                RdfHelper.getDescription(model, model.getResource("http://example.org/example1")));
        Assert.assertEquals("Comment for resource 3",
                RdfHelper.getDescription(model, model.getResource("http://example.org/example3")));

        Assert.assertTrue((new HashSet<String>(Arrays.asList("Comment 1", "Comment for resource 3"))
                .contains(RdfHelper.getDescription(model, null))));

        Assert.assertNull(RdfHelper.getDescription(model, model.getResource("http://example.org/example2")));
        Assert.assertNull(RdfHelper.getDescription(model, model.getResource("http://example.org/example4")));
        Assert.assertNull(RdfHelper.getDescription(null, model.getResource("http://example.org/example1")));
    }

    @Test
    public void testGetStringValue() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                "value 1");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2"),
                model.getResource("http://example.org/example2"));
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "A");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "B");

        // literal object matches
        Assert.assertEquals("value 1", RdfHelper.getStringValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource object matches
        Assert.assertEquals("http://example.org/example2", RdfHelper.getStringValue(model,
                model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2")));
        // more than one triple matches
        Assert.assertTrue((new HashSet<String>(Arrays.asList("A", "B"))).contains(RdfHelper.getStringValue(model,
                model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"))));

        // resource wildcard
        Assert.assertTrue((new HashSet<String>(Arrays.asList("A", "B")))
                .contains(RdfHelper.getStringValue(model, null, model.getProperty("http://example.org/property3"))));
        // property wildcard
        Assert.assertTrue((new HashSet<String>(Arrays.asList("value 1", "http://example.org/example2", "A", "B")))
                .contains(RdfHelper.getStringValue(model, model.getResource("http://example.org/example1"), null)));

        // resource and property exist but there is no matching triple
        Assert.assertNull(RdfHelper.getStringValue(model, model.getResource("http://example.org/example2"),
                model.getProperty("http://example.org/property1")));
        // resource does not exist
        Assert.assertNull(RdfHelper.getStringValue(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://example.org/property1")));
        // property does not exist
        Assert.assertNull(RdfHelper.getStringValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property4")));
        // model is null
        Assert.assertNull(RdfHelper.getStringValue(null, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
    }

    @Test
    public void testGetIntValue() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                "1");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2"),
                "32768");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "2");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "3");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                "a");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                model.getResource("http://example.org/example1"));
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                model.getResource("not an int value"));

        // literal object matches
        Assert.assertEquals(1, (int) RdfHelper.getIntValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource object matches
        Assert.assertEquals(32768, (int) RdfHelper.getIntValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property2")));
        // more than one triple matches
        Assert.assertTrue((new HashSet<Integer>(Arrays.asList(2, 3))).contains(RdfHelper.getIntValue(model,
                model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"))));

        // resource wildcard
        Assert.assertTrue((new HashSet<Integer>(Arrays.asList(2, 3)))
                .contains(RdfHelper.getIntValue(model, null, model.getProperty("http://example.org/property3"))));
        // property wildcard

        Assert.assertTrue((new HashSet<Integer>(Arrays.asList(1, 32768, 2, 3)))
                .contains(RdfHelper.getIntValue(model, model.getResource("http://example.org/example1"), null)));

        // resource and property exist but there is no matching triple
        Assert.assertNull(RdfHelper.getIntValue(model, model.getResource("http://example.org/example2"),
                model.getProperty("http://example.org/property1")));
        // resource does not exist
        Assert.assertNull(RdfHelper.getIntValue(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://example.org/property1")));
        // property does not exist
        Assert.assertNull(RdfHelper.getIntValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property4")));
        // model is null
        Assert.assertNull(RdfHelper.getIntValue(null, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));

        // object is resource instead of Integer
        Assert.assertNull(RdfHelper.getIntValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property4")));

        // object is a literal of a different data type instead of an Integer
        Assert.assertNull(RdfHelper.getIntValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property5")));

    }

    @Test
    public void testGetShortValue() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                "1");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2"),
                "32767");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "2");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "3");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                "a");

        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                model.getResource("http://example.org/example1"));
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                model.getResource("32769"));

        // literal object matches
        Assert.assertEquals(1, (short) RdfHelper.getShortValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource object matches Checking Short corner value
        Assert.assertEquals(32767, (short) RdfHelper.getShortValue(model,
                model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2")));

        // resource and property exist but there is no matching triple
        Assert.assertNull(RdfHelper.getShortValue(model, model.getResource("http://example.org/example2"),
                model.getProperty("http://example.org/property1")));
        // resource does not exist
        Assert.assertNull(RdfHelper.getShortValue(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://example.org/property1")));
        // property does not exist
        Assert.assertNull(RdfHelper.getShortValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property4")));
        // model is null
        Assert.assertNull(RdfHelper.getShortValue(null, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));

        // object is resource instead of Short
        Assert.assertNull(RdfHelper.getShortValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property4")));
        // object is a literal of a different data type instead of short
        Assert.assertNull(RdfHelper.getShortValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property5")));

    }

    @Test
    public void testGetLongValue() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                "1");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2"),
                "2147483649");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "2");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "3");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                "a");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                model.getResource("http://example.org/example1"));
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                model.getResource("not a long value"));

        // literal object matches
        Assert.assertEquals(1, (long) RdfHelper.getLongValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource object matches
        Assert.assertEquals(2147483649L, (long) RdfHelper.getLongValue(model,
                model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2")));

        // resource and property exist but there is no matching triple
        Assert.assertNull((RdfHelper.getLongValue(model, model.getResource("http://example.org/example2"),
                model.getProperty("http://example.org/property1"))));
        // resource does not exist
        Assert.assertNull((RdfHelper.getLongValue(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://example.org/property1"))));
        // property does not exist
        Assert.assertNull(RdfHelper.getLongValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property4")));
        // model is null
        Assert.assertNull(RdfHelper.getLongValue(null, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // object is resource instead of Long
        Assert.assertNull(RdfHelper.getLongValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property4")));
        // object is a literal of a different data type instead of long
        Assert.assertNull(RdfHelper.getLongValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property5")));

    }

    @Test
    public void testGetByteValue() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                "1");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2"),
                "127");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "2");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "3");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                model.getResource("http://example.org/example1"));
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                model.getResource("not a byte value"));

        // literal object matches
        Assert.assertEquals(1, (byte) RdfHelper.getByteValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource object matches Checking Byte corner value
        Assert.assertEquals(127, (byte) RdfHelper.getByteValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property2")));

        // resource and property exist but there is no matching triple
        Assert.assertNull(RdfHelper.getByteValue(model, model.getResource("http://example.org/example2"),
                model.getProperty("http://example.org/property1")));
        // resource does not exist
        Assert.assertNull(RdfHelper.getByteValue(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://example.org/property1")));
        // property does not exist
        Assert.assertNull(RdfHelper.getByteValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property4")));
        // model is null
        Assert.assertNull(RdfHelper.getByteValue(null, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // object is resource instead of Byte
        Assert.assertNull(RdfHelper.getByteValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property4")));
        // object is a literal of a different data type instead of byte
        Assert.assertNull(RdfHelper.getByteValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property5")));

    }

    @Test
    public void testGetDateValue() {
        String modelString = "<http://example.org/MyChallenge> a <http://w3id.org/hobbit/vocab#Challenge>;"
                + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#label> \"My example Challenge\"@en;"
                + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#comment>    \"This is an example for a challenge.\"@en;"
                + "<http://w3id.org/hobbit/vocab#executionDate> \"2016-12-24\"^^<http://www.w3.org/2001/XMLSchema#Date>;"
                + "<http://w3id.org/hobbit/vocab#publicationDate> \"2016-12-26\"^^<http://www.w3.org/2001/XMLSchema#Date> ."
                + "<http://example.org/MySecondChallenge> <http://w3id.org/hobbit/vocab#publicationDate> \"2016-12-27\"^^<http://www.w3.org/2001/XMLSchema#Date> .";
        Model model = ModelFactory.createDefaultModel();
        model.read(new StringReader(modelString), "http://example.org/", "TTL");

        TimeZone timeZone = TimeZone.getTimeZone("GMT");
        Calendar executionDate = Calendar.getInstance(timeZone);
        executionDate.set(2016, Calendar.DECEMBER, 24, 0, 0, 0);
        executionDate.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(executionDate,
                RdfHelper.getDateValue(model, model.getResource("http://example.org/MyChallenge"),
                        model.getProperty("http://w3id.org/hobbit/vocab#executionDate")));
        Calendar publicationDate = Calendar.getInstance(timeZone);
        publicationDate.set(2016, Calendar.DECEMBER, 26, 0, 0, 0);
        publicationDate.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(publicationDate,
                RdfHelper.getDateValue(model, model.getResource("http://example.org/MyChallenge"),
                        model.getProperty("http://w3id.org/hobbit/vocab#publicationDate")));
        Calendar secPublicationDate = Calendar.getInstance(timeZone);
        secPublicationDate.set(2016, Calendar.DECEMBER, 27, 0, 0, 0);
        secPublicationDate.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(secPublicationDate,
                RdfHelper.getDateValue(model, model.getResource("http://example.org/MySecondChallenge"),
                        model.getProperty("http://w3id.org/hobbit/vocab#publicationDate")));

        Assert.assertTrue((new HashSet<Calendar>(Arrays.asList(publicationDate, secPublicationDate))).contains(RdfHelper
                .getDateValue(model, null, model.getProperty("http://w3id.org/hobbit/vocab#publicationDate"))));

        // resource and property exist but there is no matching triple
        Assert.assertNull(RdfHelper.getDateValue(model, model.getResource("http://example.org/MySecondChallenge"),
                model.getProperty("http://w3id.org/hobbit/vocab#executionDate")));
        // resource does not exist
        Assert.assertNull(RdfHelper.getDateValue(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://w3id.org/hobbit/vocab#executionDate")));
        // property does not exist
        Assert.assertNull(RdfHelper.getDateValue(model, model.getResource("http://example.org/MySecondChallenge"),
                model.getProperty("http://example.org/property4")));
        // model is null
        Assert.assertNull(RdfHelper.getDateValue(null, model.getResource("http://example.org/MyChallenge"),
                model.getProperty("http://w3id.org/hobbit/vocab#executionDate")));
    }

    @Test
    public void testGetDateTimeValue() {
        String modelString = "<http://example.org/MyChallenge> "
                + "<http://w3id.org/hobbit/vocab#registrationCutoffDate> \"2016-12-24T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> .";
        Model model = ModelFactory.createDefaultModel();
        model.read(new StringReader(modelString), "http://example.org/", "TTL");

        TimeZone timeZone = TimeZone.getTimeZone("GMT");
        Calendar executionDate = Calendar.getInstance(timeZone);
        executionDate.set(2016, Calendar.DECEMBER, 24, 0, 0, 0);
        executionDate.set(Calendar.MILLISECOND, 0);
        Assert.assertEquals(executionDate,
                RdfHelper.getDateTimeValue(model, model.getResource("http://example.org/MyChallenge"),
                        model.getProperty("http://w3id.org/hobbit/vocab#registrationCutoffDate")));
    }

    @Test
    public void testGetDurationValue() {
        StringBuilder modelBuilder = new StringBuilder();
        modelBuilder.append(
                "<http://example.org/full>      <http://example.org/d> \"P2Y6M5DT12H35M30S\"^^<http://www.w3.org/2001/XMLSchema#duration> .");
        modelBuilder.append(
                "<http://example.org/some>      <http://example.org/d> \"P1DT2H\"^^<http://www.w3.org/2001/XMLSchema#duration> .");
        modelBuilder.append(
                "<http://example.org/overflow>  <http://example.org/d> \"PT30H\"^^<http://www.w3.org/2001/XMLSchema#duration> .");
        modelBuilder.append(
                "<http://example.org/minutes>   <http://example.org/d> \"PT20M\"^^<http://www.w3.org/2001/XMLSchema#duration> .");
        modelBuilder.append(
                "<http://example.org/withZeros> <http://example.org/d> \"P0M20D\"^^<http://www.w3.org/2001/XMLSchema#duration> .");
        modelBuilder.append(
                "<http://example.org/exactZero> <http://example.org/d> \"P0D\"^^<http://www.w3.org/2001/XMLSchema#duration> .");
        modelBuilder.append(
                "<http://example.org/negative>  <http://example.org/d> \"-P60D\"^^<http://www.w3.org/2001/XMLSchema#duration> .");
        modelBuilder.append(
                "<http://example.org/fraction>  <http://example.org/d> \"PT1M30.5S\"^^<http://www.w3.org/2001/XMLSchema#duration> .");
        Model model = ModelFactory.createDefaultModel();
        model.read(new StringReader(modelBuilder.toString()), "http://example.org/", "TTL");

        Assert.assertEquals(Duration.of(26, ChronoUnit.HOURS), RdfHelper.getDurationValue(model,
                model.getResource("http://example.org/some"), model.getProperty("http://example.org/d")));

        Assert.assertEquals(Duration.of(30, ChronoUnit.HOURS), RdfHelper.getDurationValue(model,
                model.getResource("http://example.org/overflow"), model.getProperty("http://example.org/d")));

        Assert.assertEquals(Duration.of(20, ChronoUnit.MINUTES), RdfHelper.getDurationValue(model,
                model.getResource("http://example.org/minutes"), model.getProperty("http://example.org/d")));

        Assert.assertEquals(Duration.ZERO, RdfHelper.getDurationValue(model,
                model.getResource("http://example.org/exactZero"), model.getProperty("http://example.org/d")));

        Assert.assertEquals(Duration.of(-60, ChronoUnit.DAYS), RdfHelper.getDurationValue(model,
                model.getResource("http://example.org/negative"), model.getProperty("http://example.org/d")));

        Assert.assertEquals(Duration.of(90500, ChronoUnit.MILLIS), RdfHelper.getDurationValue(model,
                model.getResource("http://example.org/fraction"), model.getProperty("http://example.org/d")));
    }

    @Test
    public void testGetBooleanValue() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                "true");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2"),
                "false");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "false");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                "true");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                "false");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                "a");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                model.getResource("http://example.org/example1"));

        // model is null
        Assert.assertNull(RdfHelper.getBooleanValue(null, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource and property exist but there is no matching triple
        Assert.assertNull(RdfHelper.getBooleanValue(model, model.getResource("http://example.org/example2"),
                model.getProperty("http://example.org/property1")));
        // resource does not exist
        Assert.assertNull(RdfHelper.getBooleanValue(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://example.org/property1")));
        // property does not exist
        Assert.assertNull(RdfHelper.getBooleanValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property0")));
        // object is resource instead of Boolean
        Assert.assertNull(RdfHelper.getBooleanValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property5")));

        // literal object matches
        Assert.assertEquals(true, RdfHelper.getBooleanValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource object matches
        Assert.assertEquals(false, RdfHelper.getBooleanValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property2")));
        // more than one triple matches
        Assert.assertTrue((new HashSet<Boolean>(Arrays.asList(true, false))).contains(RdfHelper.getBooleanValue(model,
                model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"))));

        // resource wildcard
        Assert.assertTrue((new HashSet<Boolean>(Arrays.asList(false, true)))
                .contains(RdfHelper.getBooleanValue(model, null, model.getProperty("http://example.org/property4"))));
        // property wildcard
        Assert.assertTrue((new HashSet<Boolean>(Arrays.asList(true, false, false, true)))
                .contains(RdfHelper.getBooleanValue(model, model.getResource("http://example.org/example1"), null)));

    }

    @Test
    public void testGetFloatValue() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                "7.435f");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2"),
                "70.7976f");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "31.87f");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                "456.34578f");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                "94.454f");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                "a");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                model.getResource("http://example.org/example1"));

        // model is null
        Assert.assertNull(RdfHelper.getFloatValue(null, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource and property exist but there is no matching triple
        Assert.assertNull(RdfHelper.getFloatValue(model, model.getResource("http://example.org/example2"),
                model.getProperty("http://example.org/property1")));
        // resource does not exist
        Assert.assertNull(RdfHelper.getFloatValue(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://example.org/property1")));
        // property does not exist
        Assert.assertNull(RdfHelper.getFloatValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property0")));
        // object is resource instead of Float
        Assert.assertNull(RdfHelper.getFloatValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property5")));

        // literal object matches
        Assert.assertEquals(7.435f, RdfHelper.getFloatValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")), DELTA);
        // resource object matches
        Assert.assertEquals(70.7976f, RdfHelper.getFloatValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property2")), DELTA);
        // more than one triple matches
        Assert.assertTrue((new HashSet<>(Arrays.asList(456.34578f, 94.454f))).contains(RdfHelper.getFloatValue(model,
                model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"))));

        // resource wildcard
        Assert.assertTrue((new HashSet<>(Arrays.asList(456.34578f, 94.454f)))
                .contains(RdfHelper.getFloatValue(model, null, model.getProperty("http://example.org/property4"))));
        // property wildcard
        Assert.assertTrue((new HashSet<>(Arrays.asList(7.435f, 70.7976f, 31.87f, 456.34578f, 94.454f)))
                .contains(RdfHelper.getFloatValue(model, model.getResource("http://example.org/example1"), null)));

    }

    @Test
    public void testGetDoubleValue() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                "123.43555");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2"),
                "1.7976931348623157E308");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "56.87643872658737565987395875");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                "674.34657634589E101");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                "830.454589E101");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                "a");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                model.getResource("http://example.org/example1"));
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property6"),
                model.getResource("false"));

        // model is null
        Assert.assertNull(RdfHelper.getDoubleValue(null, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource and property exist but there is no matching triple
        Assert.assertNull(RdfHelper.getDoubleValue(model, model.getResource("http://example.org/example2"),
                model.getProperty("http://example.org/property1")));
        // resource does not exist
        Assert.assertNull(RdfHelper.getDoubleValue(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://example.org/property1")));
        // property does not exist
        Assert.assertNull(RdfHelper.getDoubleValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property0")));
        // object is resource instead of Double
        Assert.assertNull(RdfHelper.getDoubleValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property5")));
        // object is a literal of a different data type instead of double
        Assert.assertNull(RdfHelper.getDoubleValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property6")));

        // literal object matches
        Assert.assertEquals(123.43555, RdfHelper.getDoubleValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")), DELTA);
        // resource object matches
        Assert.assertEquals(1.7976931348623157E308, RdfHelper.getDoubleValue(model,
                model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2")),
                DELTA);
        // more than one triple matches
        Assert.assertTrue((new HashSet<>(Arrays.asList(674.34657634589E101, 830.454589E101)))
                .contains(RdfHelper.getDoubleValue(model, model.getResource("http://example.org/example1"),
                        model.getProperty("http://example.org/property4"))));

        // resource wildcard
        Assert.assertTrue((new HashSet<>(Arrays.asList(674.34657634589E101, 830.454589E101)))
                .contains(RdfHelper.getDoubleValue(model, null, model.getProperty("http://example.org/property4"))));
        // property wildcard
        Assert.assertTrue((new HashSet<>(Arrays.asList(123.43555, 1.7976931348623157E308, 56.87643872658737565987395875,
                674.34657634589E101, 830.454589E101))).contains(
                        RdfHelper.getDoubleValue(model, model.getResource("http://example.org/example1"), null)));

    }

    @Test
    public void testGetCharValue() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                "2");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2"),
                "z");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property3"),
                "r");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                "8");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"),
                "o");
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property5"),
                model.getResource("http://example.org/example1"));
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property6"),
                model.getResource("not a char value"));

        // model is null
        Assert.assertNull(RdfHelper.getCharValue(null, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource and property exist but there is no matching triple
        Assert.assertNull(RdfHelper.getCharValue(model, model.getResource("http://example.org/example2"),
                model.getProperty("http://example.org/property1")));
        // resource does not exist
        Assert.assertNull(RdfHelper.getCharValue(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://example.org/property1")));
        // property does not exist
        Assert.assertNull(RdfHelper.getCharValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property0")));
        // object is resource instead of Character
        Assert.assertNull(RdfHelper.getCharValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property5")));
        // object is a literal of a different data type instead of char
        Assert.assertNull(RdfHelper.getCharValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property6")));

        // literal object matches
        Assert.assertEquals('2', (char) RdfHelper.getCharValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")));
        // resource object matches
        Assert.assertEquals('z', (char) RdfHelper.getCharValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property2")));
        // more than one triple matches
        Assert.assertTrue((new HashSet<>(Arrays.asList('8', 'o'))).contains(RdfHelper.getCharValue(model,
                model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property4"))));
        // literal Value not char
        Assert.assertNotEquals(8, (char) RdfHelper.getCharValue(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property4")));
        // resource wildcard
        Assert.assertTrue((new HashSet<>(Arrays.asList('8', 'o')))
                .contains(RdfHelper.getCharValue(model, null, model.getProperty("http://example.org/property4"))));
        // property wildcard
        Assert.assertTrue((new HashSet<>(Arrays.asList('2', 'z', 'r', '8', 'o')))
                .contains(RdfHelper.getCharValue(model, model.getResource("http://example.org/example1"), null)));

    }

    @Test
    public void testGetObjectNodes() {
        Model model = ModelFactory.createDefaultModel();
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                model.getResource("http://example.org/example2"));
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property1"),
                model.getResource("http://example.org/example3"));
        model.add(model.getResource("http://example.org/example1"), model.getProperty("http://example.org/property2"),
                model.getResource("http://example.org/example3"));
        model.add(model.getResource("http://example.org/example2"), model.getProperty("http://example.org/property1"),
                model.getResource("http://example.org/example4"));
        model.add(model.getResource("http://example.org/example2"), model.getProperty("http://example.org/property1"),
                model.getResource("http://example.org/example5"));
        model.add(model.getResource("http://example.org/example3"), model.getProperty("http://example.org/property3"),
                "This is a literal");

        // model is null
        Assert.assertEquals(0, RdfHelper.getObjectNodes(null, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")).size());
        // resource and property exist but there is no matching triple
        Assert.assertEquals(0, RdfHelper.getObjectNodes(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property3")).size());
        // resource does not exist
        Assert.assertEquals(0, RdfHelper.getObjectNodes(model, model.getResource("http://example.org/example0"),
                model.getProperty("http://example.org/property3")).size());
        // property does not exist
        Assert.assertEquals(0, RdfHelper.getObjectNodes(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property0")).size());

        RDFNode[] result;
        Comparator<RDFNode> comparator = new Comparator<RDFNode>() {
            @Override
            public int compare(RDFNode arg0, RDFNode arg1) {
                return arg0.toString().compareTo(arg1.toString());
            }
        };
        // list with two results
        result = RdfHelper.getObjectNodes(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property1")).toArray(new RDFNode[0]);
        Arrays.sort(result,comparator);
        Assert.assertArrayEquals(new RDFNode[] { model.getResource("http://example.org/example2"),
                model.getResource("http://example.org/example3") }, result);
        // list with single result
        result = RdfHelper.getObjectNodes(model, model.getResource("http://example.org/example1"),
                model.getProperty("http://example.org/property2")).toArray(new RDFNode[0]);
        Assert.assertArrayEquals(new RDFNode[] { model.getResource("http://example.org/example3") }, result);
        // object is literal instead of a resource
        result = RdfHelper.getObjectNodes(model, model.getResource("http://example.org/example3"),
                model.getProperty("http://example.org/property3")).toArray(new RDFNode[0]);
        Assert.assertArrayEquals(new RDFNode[] { model.createLiteral("This is a literal") }, result);
        // resource wildcard
        result = RdfHelper.getObjectNodes(model, null, model.getProperty("http://example.org/property1"))
                .toArray(new RDFNode[0]);
        Arrays.sort(result,comparator);
        Assert.assertArrayEquals(new RDFNode[] { model.getResource("http://example.org/example2"),
                model.getResource("http://example.org/example3"), model.getResource("http://example.org/example4"),
                model.getResource("http://example.org/example5") }, result);
        // property wildcard
        result = RdfHelper.getObjectNodes(model, model.getResource("http://example.org/example1"), null)
                .toArray(new RDFNode[0]);
        Arrays.sort(result,comparator);
        Assert.assertArrayEquals(new RDFNode[] { model.getResource("http://example.org/example2"),
                model.getResource("http://example.org/example3") },
                result);
    }
}
