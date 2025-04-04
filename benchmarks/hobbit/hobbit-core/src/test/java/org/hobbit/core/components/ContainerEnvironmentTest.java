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
package org.hobbit.core.components;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.hobbit.core.components.dummy.DummyCommandReceivingComponent;
import org.hobbit.utils.config.HobbitConfiguration;
import org.hobbit.core.Constants;
import org.hobbit.core.TestConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;
import static org.junit.Assert.*;


@RunWith(Parameterized.class)
public class ContainerEnvironmentTest {

    private AbstractCommandReceivingComponent component;

    @Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {
                // input
                null,
                // expected output
                new String[] {
                    Constants.RABBIT_MQ_HOST_NAME_KEY + "=" + TestConstants.RABBIT_HOST,
                    Constants.HOBBIT_SESSION_ID_KEY + "=0",
                }
            },
            {
                // input
                new String[] {},
                // expected output
                new String[] {
                    Constants.RABBIT_MQ_HOST_NAME_KEY + "=" + TestConstants.RABBIT_HOST,
                    Constants.HOBBIT_SESSION_ID_KEY + "=0",
                }
            },
            {
                // input
                new String[] {
                    "A=B",
                    "C=",
                },
                // expected output
                new String[] {
                    "A=B",
                    "C=",
                    Constants.RABBIT_MQ_HOST_NAME_KEY + "=" + TestConstants.RABBIT_HOST,
                    Constants.HOBBIT_SESSION_ID_KEY + "=0",
                }
            },
            {
                // input
                new String[] {
                    Constants.RABBIT_MQ_HOST_NAME_KEY + "=another.rabbit",
                },
                // expected output
                new String[] {
                    Constants.RABBIT_MQ_HOST_NAME_KEY + "=another.rabbit",
                    Constants.HOBBIT_SESSION_ID_KEY + "=0",
                }
            },
            {
                // input
                new String[] {
                    "A=B",
                    Constants.RABBIT_MQ_HOST_NAME_KEY + "=another.rabbit",
                    "C=",
                },
                // expected output
                new String[] {
                    "A=B",
                    Constants.RABBIT_MQ_HOST_NAME_KEY + "=another.rabbit",
                    "C=",
                    Constants.HOBBIT_SESSION_ID_KEY + "=0",
                }
            },
        });
    }

    @Parameter(0)
    public String[] passedEnvVariables;

    @Parameter(1)
    public String[] expectedEnvVariables;

    @Before
    public void setUp() throws Exception {
        Configuration configurationVar = new PropertiesConfiguration();

        configurationVar.addProperty(Constants.RABBIT_MQ_HOST_NAME_KEY, TestConstants.RABBIT_HOST);
        configurationVar.addProperty(Constants.HOBBIT_SESSION_ID_KEY, "0");
        HobbitConfiguration configVar = new HobbitConfiguration();
        configVar.addConfiguration(configurationVar);
        component = new DummyCommandReceivingComponent(configVar);
        component.init();
    }

    @After
    public void tearDown() throws Exception {
        component.close();
    }

    @Test
    public void test() throws Exception {
        assertArrayEquals(expectedEnvVariables, component.extendContainerEnvVariables(passedEnvVariables));
    }
}
