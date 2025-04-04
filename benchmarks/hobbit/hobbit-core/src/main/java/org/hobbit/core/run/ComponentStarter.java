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
package org.hobbit.core.run;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.configuration2.EnvironmentConfiguration;
import org.apache.commons.io.IOUtils;
import org.hobbit.core.Constants;
import org.hobbit.core.components.AbstractCommandReceivingComponent;
import org.hobbit.core.components.Component;
import org.hobbit.utils.config.HobbitConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains the main method starting a given {@link Component}.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class ComponentStarter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComponentStarter.class);

    private static Component component;

    private static boolean closed = false;

    /**
     * This is the main method creating and starting an instance of a
     * {@link Component} with the given class name.
     *
     * @param args The first element has to be the class name of the component.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            LOGGER.error("Not enough arguments. The name of a class implementing the "
                    + Component.class.getCanonicalName() + " interface was expected.");
            System.exit(Constants.COMPONENT_STARTER_ERROR_EXIT_CODE);
        }
        addShutdownHook();
        boolean success = true;
        try {
            component = createComponentInstance(args[0]);
            // set configuration
            configure(component);
            // initialize the component
            component.init();
            // run the component
            component.run();
        } catch (Throwable t) {
            LOGGER.error("Exception while executing component. Exiting with error code.", t);
            reportErrorIfPossible(t);
            success = false;
        } finally {
            closeComponent();
        }

        if (!success) {
            System.exit(Constants.COMPONENT_STARTER_ERROR_EXIT_CODE);
        } else if (forceTermination()) {
            System.exit(0);
        }
    }

    /**
     * This method simply creates an instance of the given class by calling a
     * constructor that needs no arguments and cats the newly created instance into
     * a {@link Component} instance. Note that this method assumes that a) there is
     * a constructor that needs no arguments to be executed and b) the class with
     * the given name is implementing the {@link Constructor} interface.
     *
     * @param className the name of the class implementing the {@link Component}
     *                  interface.
     * @return an instance of that class.
     * @throws ClassNotFoundException    - If the class with the given name can not
     *                                   be found.
     * @throws NoSuchMethodException     - If there is no constructor that needs no
     *                                   parameters.
     * @throws SecurityException         - If the constructor can not be accessed
     *                                   because of security policies.
     * @throws InstantiationException    - If the class with the given class name
     *                                   represents an abstract class.
     * @throws IllegalAccessException    - If the Constructor object is enforcing
     *                                   Java language access control and the
     *                                   underlying constructor is inaccessible.
     * @throws IllegalArgumentException  - If the number of actual and formal
     *                                   parameters differ; if an unwrapping
     *                                   conversion for primitive arguments fails;
     *                                   or if, after possible unwrapping, a
     *                                   parameter value cannot be converted to the
     *                                   corresponding formal parameter type by a
     *                                   method invocation conversion; if this
     *                                   constructor pertains to an enum type.
     *                                   (Should not occur)
     * @throws InvocationTargetException - If the constructor throws an exception.
     */
    private static Component createComponentInstance(String className)
            throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Class<?> componentClass = ClassLoader.getSystemClassLoader().loadClass(className);
        Constructor<?> constructor = componentClass.getConstructor();
        return Component.class.cast(constructor.newInstance());
    }

    private static void configure(Component component) {
        HobbitConfiguration configuration = new HobbitConfiguration();
        configuration.addConfiguration(new EnvironmentConfiguration());
        // Add more configurations if necessary

        component.setConfiguration(configuration);
    }

    private static synchronized void closeComponent() {
        if (!closed) {
            IOUtils.closeQuietly(component);
            closed = true;
        }
    }

    private static void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                closeComponent();
            }
        });
    }

    private static boolean forceTermination() {
        Map<String, String> env = System.getenv();
        if (env.containsKey(Constants.COMPONENT_STARTER_FORCE_EXIT_WHEN_TERMINATING_ENV_KEY)) {
            String value = env.get(Constants.COMPONENT_STARTER_FORCE_EXIT_WHEN_TERMINATING_ENV_KEY);
            try {
                return Boolean.parseBoolean(value);
            } catch (Exception e) {
                LOGGER.error("Couldn't parse value of "
                        + Constants.COMPONENT_STARTER_FORCE_EXIT_WHEN_TERMINATING_ENV_KEY + ". It will be ignored.", e);
            }
        }
        return false;
    }

    private static void reportErrorIfPossible(Throwable t) {
        if (component instanceof AbstractCommandReceivingComponent && t instanceof Exception) {
            AbstractCommandReceivingComponent crComponent = (AbstractCommandReceivingComponent) component;
            crComponent.reportUnhandledExceptionSavely((Exception) t);
        }
    }
}
