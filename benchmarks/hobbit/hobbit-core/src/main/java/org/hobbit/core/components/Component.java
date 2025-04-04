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

import java.io.Closeable;

import org.hobbit.utils.config.HobbitConfiguration;

/**
 * The basic interface of a hobbit component.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public interface Component extends Closeable {

    /**
     * This method initializes the component.
     *
     * @throws Exception if an error occurs during the initialization
     */
    public void init() throws Exception;

    /**
     * This method executes the component.
     *
     * @throws Exception if an error occurs during the execution
     */
    public void run() throws Exception;

    /**
     * Setter for the configuration that the component should make use of.
     * 
     * @param configuration the configuration that is used by this component
     */
    public void setConfiguration(HobbitConfiguration configuration);
}
