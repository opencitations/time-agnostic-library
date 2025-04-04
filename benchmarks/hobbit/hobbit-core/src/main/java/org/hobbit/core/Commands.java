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

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

public final class Commands {

    private Commands() {
    }

    /**
     * The signal sent by the benchmarked system to indicate that the system is
     * ready.
     */
    public static final byte SYSTEM_READY_SIGNAL = 1;
    /**
     * The signal sent by the benchmark controller to indicate that the benchmark is
     * ready.
     */
    public static final byte BENCHMARK_READY_SIGNAL = 2;
    /**
     * The signal sent by the data generator to indicate that it is ready.
     */
    public static final byte DATA_GENERATOR_READY_SIGNAL = 3;
    /**
     * The signal sent by the task generator to indicate that it is ready.
     */
    public static final byte TASK_GENERATOR_READY_SIGNAL = 4;
    /**
     * The signal sent by the evaluation storage to indicate that it is ready.
     */
    public static final byte EVAL_STORAGE_READY_SIGNAL = 5;
    /**
     * The signal sent by the evaluation module to indicate that it is ready.
     */
    public static final byte EVAL_MODULE_READY_SIGNAL = 6;

    public static final byte DATA_GENERATOR_START_SIGNAL = 7;

    public static final byte TASK_GENERATOR_START_SIGNAL = 8;

    public static final byte EVAL_MODULE_FINISHED_SIGNAL = 9;

    public static final byte EVAL_STORAGE_TERMINATE = 10;

    public static final byte BENCHMARK_FINISHED_SIGNAL = 11;
    /**
     * Command used to ask a docker managing component to start a certain container.
     * <p>
     * The command is followed by a String containing the following JSON data: <br>
     * <code>
     * {<br>"image": "image-to-run",<br> "type": "system|benchmark",<br> "parent":"parent-container-id"<br>}
     * </code>
     * </p>
     */
    public static final byte DOCKER_CONTAINER_START = 12;
    /**
     * Command used to ask a docker managing component to stop a certain container.
     * <p>
     * The command is followed by a String containing the following JSON data: <br>
     * <code>
     * {<br>"containerId": "container-to-stop"<br>}
     * </code>
     * </p>
     */
    public static final byte DOCKER_CONTAINER_STOP = 13;

    public static final byte DATA_GENERATION_FINISHED = 14;

    public static final byte TASK_GENERATION_FINISHED = 15;

    public static final byte DOCKER_CONTAINER_TERMINATED = 16;

    public static final byte START_BENCHMARK_SIGNAL = 17;

    public static final byte REQUEST_SYSTEM_RESOURCES_USAGE = 18;
    /**
     * Command used to report an error that should be persisted as part of the
     * experiment result data.
     * <p>
     * The command is followed by a String containing the following JSON data: <br>
     * <code>
     * {<br>"containerId": "container reporting the error",
     * <br>"errorType": "IRI of the error type (optional)",
     * <br>"label": "A string that can be used as short label of an error (optional, the error type label will be used as default)"
     * <br>"description": "A string that can be used as a short description of an error (optional, the error type description will be used as default)"
     * <br>"details": "A string that contains details about the error, e.g., a stack trace (optional)."
     *   <br>}
     * </code>
     * </p>
     * <p>
     * The {@link org.hobbit.core.data.ErrorData} class can be used to represent
     * this data as Java object.</p>
     */
    public static final byte REPORT_ERROR = 19;

    private static final ImmutableMap<Byte, String> ID_TO_COMMAND_NAME_MAP = generateMap();

    private static ImmutableMap<Byte, String> generateMap() {
        Map<Byte, String> mapping = new HashMap<Byte, String>();
        Class<Commands> clazz = Commands.class;
        Field[] fields = clazz.getFields();
        byte commandId;
        for (int i = 0; i < fields.length; ++i) {
            try {
                commandId = fields[i].getByte(null);
                mapping.put(commandId, fields[i].getName());
            } catch (Exception e) {
            }
        }
        return ImmutableMap.copyOf(mapping);
    }

    /**
     * Returns the name of the command if it is defined inside the {@link Commands}
     * class or its id as String.
     *
     * @param command the command that should be transformed into a String
     * @return the name of the command or its id if the name is not known
     */
    public static String toString(byte command) {
        Byte commandObject = Byte.valueOf(command);
        if (Commands.ID_TO_COMMAND_NAME_MAP.containsKey(commandObject)) {
            return ID_TO_COMMAND_NAME_MAP.get(commandObject);
        } else {
            return Byte.toString(command);
        }
    }
}
