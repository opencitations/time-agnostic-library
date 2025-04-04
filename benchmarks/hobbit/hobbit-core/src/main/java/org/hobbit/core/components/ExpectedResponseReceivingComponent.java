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
 * This interface is implemented by components that want to receive the expected
 * responses from the task generator component.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public interface ExpectedResponseReceivingComponent extends Component {

    /**
     * This method is called if an expected response is received from a task
     * generator component.
     *
     * @param taskId
     *            the id of the task
     * @param timestamp
     *            the time at which the task has been sent to the system
     * @param data
     *            the data received from a task generator
     */
    public void receiveExpectedResponseData(String taskId, long timestamp, byte[] data);

}
