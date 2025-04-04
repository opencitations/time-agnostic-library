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

/**
 * This interface is implemented by components that want to receive the
 * responses from the system.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public interface ResponseReceivingComponent extends Component {

    /**
     * This method is called if a response is received from the system.
     *
     * @param taskId
     *            the id of the task
     * @param timestamp
     *            the time at which the response has been received from the
     *            system
     * @param data
     *            the data received from a data generator
     */
    public void receiveResponseData(String taskId, long timestamp, byte[] data);

}
