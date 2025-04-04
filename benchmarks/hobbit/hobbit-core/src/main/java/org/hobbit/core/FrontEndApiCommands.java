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
package org.hobbit.core;

/**
 * Command bytes that can be sent to the controller. The response depends on the
 * used command.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class FrontEndApiCommands {

    /**
     * This command requests
     * <ul>
     * <li>the currently running experiment and its status</li>
     * <li>the list of experiments in the queue</li>
     * <li>the planned challenges and their experiments</li>
     * </ul>
     */
    public static final byte LIST_CURRENT_STATUS = 0;

    /**
     * Lists the benchmarks that are currently available.
     */
    public static final byte LIST_AVAILABLE_BENCHMARKS = 1;

    /**
     * This command requests the parameters of a benchmark and the systems that can
     * be benchmarked with it.
     */
    public static final byte GET_BENCHMARK_DETAILS = 2;

    /**
     * This command adds the given benchmark system combination to the queue.
     */
    public static final byte ADD_EXPERIMENT_CONFIGURATION = 3;

    /**
     * This command requests systems that have been uploaded by the given user.
     */
    public static final byte GET_SYSTEMS_OF_USER = 4;

    /**
     * This command closes the given challenge.
     */
    public static final byte CLOSE_CHALLENGE = 5;

    /**
     * This command deletes the given experiment from the queue with the access
     * authority of the given user.
     */
    public static final byte REMOVE_EXPERIMENT = 6;
}
