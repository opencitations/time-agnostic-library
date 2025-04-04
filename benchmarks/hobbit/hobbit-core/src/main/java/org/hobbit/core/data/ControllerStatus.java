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
package org.hobbit.core.data;

/**
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 * @deprecated Use {@link org.hobbit.core.data.status.ControllerStatus} instead.
 */
@Deprecated
public class ControllerStatus {

    public String currentExperimentId;
    public String currentBenchmarkName;
    public String currentBenchmarkUri;
    public String currentSystemName;
    public String currentSystemUri;
    public String currentStatus;
    public ConfiguredExperiment queue[];

}
