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

import java.net.URI;

/**
 * A simple utility class that helps to work with docker and docker accessories.
 *
 * @author Denis Kuchelev
 *
 */
public class DockerHelper {

    /**
     * Gets hostname (or address) of the host at which docker service configured in environment runs.
     *
     * @return plain hostname (or address)
     */
    public static String getHost() {
        return getHost(System.getenv("DOCKER_HOST"));
    }

    /**
     * Gets hostname (or address) of the host at which specified docker service runs.
     *
     * @param dockerURI
     *            the URI docker runs at, typically contents of DOCKER_HOST env var.
     * @return plain hostname (or address)
     */
    public static String getHost(final String dockerURI) {
        if (dockerURI == null) {
            return "localhost";
        }

        String dockerHost = null;

        try {
            dockerHost = new URI(dockerURI).getHost();
        } catch (java.net.URISyntaxException e) {
        }

        if (dockerHost == null) {
            dockerHost = dockerURI;
        }

        return dockerHost;
    }

}
