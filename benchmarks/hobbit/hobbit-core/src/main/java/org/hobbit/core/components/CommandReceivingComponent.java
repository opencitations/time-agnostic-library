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
 * This interface is implemented by components that want to receive and process
 * commands from the hobbit command queue.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public interface CommandReceivingComponent extends Component {

    /**
     * This method is called if a command is received and might be interesting
     * for this particular component.
     *
     * @param command
     *            the byte encoding the command
     * @param data
     *            additional data that was sent together with the command
     */
    public void receiveCommand(byte command, byte[] data);

}
