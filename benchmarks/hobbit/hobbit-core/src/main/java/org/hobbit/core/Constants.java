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

import java.util.TimeZone;

/**
 * This class defines constants of the hobbit platform.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public final class Constants {

    private Constants() {
    }

    // =============== ENVIRONMENT CONSTANTS ===============

    public static final String HOBBIT_SESSION_ID_KEY = "HOBBIT_SESSION_ID";

    public static final String RABBIT_MQ_HOST_NAME_KEY = "HOBBIT_RABBIT_HOST";

    public static final String GENERATOR_ID_KEY = "HOBBIT_GENERATOR_ID";

    public static final String GENERATOR_COUNT_KEY = "HOBBIT_GENERATOR_COUNT";

    public static final String SYSTEM_URI_KEY = "HOBBIT_SYSTEM_URI";

    public static final String CONTAINER_NAME_KEY = "HOBBIT_CONTAINER_NAME";
    @Deprecated
    public static final String SYSTEM_CONTAINER_ID_KEY = "HOBBIT_SYSTEM_CONTAINER_ID";

    public static final String BENCHMARK_PARAMETERS_MODEL_KEY = "BENCHMARK_PARAMETERS_MODEL";

    public static final String SYSTEM_PARAMETERS_MODEL_KEY = "SYSTEM_PARAMETERS_MODEL";

    public static final String HOBBIT_EXPERIMENT_URI_KEY = "HOBBIT_EXPERIMENT_URI";

    public static final String DATA_QUEUE_NAME_KEY = "DATA_QUEUE_NAME";

    public static final String ACKNOWLEDGEMENT_FLAG_KEY = "ACKNOWLEDGEMENT_FLAG";

    public static final String HARDWARE_NUMBER_OF_NODES_KEY = "HOBBIT_HARDWARE_NODES";

    public static final String HARDWARE_NUMBER_OF_SYSTEM_NODES_KEY = "HOBBIT_HARDWARE_NODES_SYSTEM";

    public static final String HARDWARE_NUMBER_OF_BENCHMARK_NODES_KEY = "HOBBIT_HARDWARE_NODES_BENCHMARK";

    public static final String HARDWARE_INFO_KEY = "HOBBIT_HARDWARE_INFO";

    // =============== RABBIT CONSTANTS ===============

    /**
     * Name of the hobbit command exchange.
     */
    public static final String HOBBIT_COMMAND_EXCHANGE_NAME = "hobbit.command";

    /**
     * Name of the hobbit acknowledgement exchange.
     */
    public static final String HOBBIT_ACK_EXCHANGE_NAME = "hobbit.ack";

    /**
     * Name of the hobbit docker service queue.
     */
    @Deprecated
    public static final String DOCKER_SERVICE_QUEUE_NAME = "hobbit.docker-service";

    public static final String DATA_GEN_2_TASK_GEN_QUEUE_NAME = "hobbit.datagen-taskgen";

    public static final String DATA_GEN_2_SYSTEM_QUEUE_NAME = "hobbit.datagen-system";

    public static final String TASK_GEN_2_SYSTEM_QUEUE_NAME = "hobbit.taskgen-system";

    public static final String TASK_GEN_2_EVAL_STORAGE_DEFAULT_QUEUE_NAME = "hobbit.taskgen-evalstore";

    public static final String SYSTEM_2_EVAL_STORAGE_DEFAULT_QUEUE_NAME = "hobbit.system-evalstore";

    public static final String EVAL_MODULE_2_EVAL_STORAGE_DEFAULT_QUEUE_NAME = "hobbit.evalmod-evalstore";

    public static final String EVAL_STORAGE_2_EVAL_MODULE_DEFAULT_QUEUE_NAME = "hobbit.evalstore-evalmod";

    public static final String FRONT_END_2_CONTROLLER_QUEUE_NAME = "hobbit.frontend-controller";

    public static final String CONTROLLER_2_FRONT_END_QUEUE_NAME = "hobbit.controller-frontend";

    public static final String CONTROLLER_2_ANALYSIS_QUEUE_NAME = "hobbit.controller-analysis";

    public static final String TASK_GEN_2_EVAL_STORAGE_QUEUE_NAME_KEY = "TASK_GEN_2_EVAL_STORAGE_QUEUE_NAME";

    public static final String SYSTEM_2_EVAL_STORAGE_QUEUE_NAME_KEY = "SYSTEM_2_EVAL_STORAGE_QUEUE_NAME";

    public static final String EVAL_MODULE_2_EVAL_STORAGE_QUEUE_NAME_KEY = "EVAL_MODULE_2_EVAL_STORAGE_QUEUE_NAME";

    public static final String EVAL_STORAGE_2_EVAL_MODULE_QUEUE_NAME_KEY = "EVAL_STORAGE_2_EVAL_MODULE_QUEUE_NAME";

    public static final String STORAGE_QUEUE_NAME = "hobbit.storage";

    // =============== GRAPH CONSTANTS ===============

    /**
     * @deprecated Use org.hobbit.vocab.HobbitExperiments instead.
     */
    @Deprecated
    public static final String EXPERIMENT_URI_NS = "http://w3id.org/hobbit/experiments#";
    /**
     * @deprecated Use org.hobbit.vocab.HobbitExperiments instead.
     */
    @Deprecated
    public static final String NEW_EXPERIMENT_URI = EXPERIMENT_URI_NS + "New";
    /**
     * @deprecated Use org.hobbit.vocab.HobbitChallenges instead.
     */
    @Deprecated
    public static final String CHALLENGE_URI_NS = "http://w3id.org/hobbit/challenges#";
    /**
     * @deprecated Use org.hobbit.vocab.HobbitHardware instead.
     */
    @Deprecated
    public static final String HARDWARE_URI_NS = "http://w3id.org/hobbit/hardware#";

    public static final String PUBLIC_RESULT_GRAPH_URI = "http://hobbit.org/graphs/PublicResults";
    public static final String PRIVATE_RESULT_GRAPH_URI = "http://hobbit.org/graphs/PrivateResults";
    public static final String CHALLENGE_DEFINITION_GRAPH_URI = "http://hobbit.org/graphs/ChallengeDefinitions";

    // =============== COMPONENT STARTER CONSTANTS ===============

    /**
     * Exit code that is used if the program has to terminate because of an
     * internal error.
     */
    public static final int COMPONENT_STARTER_ERROR_EXIT_CODE = -1;
    public static final String COMPONENT_STARTER_FORCE_EXIT_WHEN_TERMINATING_ENV_KEY="COMPONENT_STARTER_FORCE_EXIT_WHEN_TERMINATING";

    // =============== OTHER CONSTANTS ===============

    public static final String HOBBIT_SESSION_ID_FOR_PLATFORM_COMPONENTS = "SYSTEM";

    public static final String HOBBIT_SESSION_ID_FOR_BROADCASTS = "BROADCAST";

    public static final String CONTAINER_TYPE_BENCHMARK = "benchmark";

    public static final String CONTAINER_TYPE_SYSTEM = "system";

    public static final String CONTAINER_TYPE_DATABASE = "data";

    public static final TimeZone DEFAULT_TIME_ZONE = TimeZone.getTimeZone("GMT");

}
