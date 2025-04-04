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
package org.hobbit.utils.docker;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class DockerHelperTest {

    private static final String ipv4 = "123.45.6.7";

    @Test
    public void getDockerHostTest() throws Exception {
        assertEquals("Should be localhost when not set",
                "localhost",
                DockerHelper.getHost(null));

        assertEquals("Should be as is when just the host is provided",
                ipv4,
                DockerHelper.getHost(ipv4));

        assertEquals("Should be just the host part when full URL is provided",
                ipv4,
                DockerHelper.getHost(String.format("tcp://%s:2376", ipv4)));
    }

}
