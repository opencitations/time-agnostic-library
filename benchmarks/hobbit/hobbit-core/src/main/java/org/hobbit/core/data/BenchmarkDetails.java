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

import java.util.List;

import org.apache.jena.rdf.model.Model;

public class BenchmarkDetails {

    public Model benchmarkModel;
    public List<SystemMetaData> systems;

    public BenchmarkDetails(Model benchmarkModel, List<SystemMetaData> systems) {
        this.benchmarkModel = benchmarkModel;
        this.systems = systems;
    }

}
