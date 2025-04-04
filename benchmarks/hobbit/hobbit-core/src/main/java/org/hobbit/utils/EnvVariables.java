package org.hobbit.utils;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.jena.rdf.model.Model;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;

/**
 * A simple class offering static access to environmental variables.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 * 
 * @deprecated Use {@link org.hobbit.utils.config.HobbitConfiguration} instead.
 *
 */
@Deprecated
public class EnvVariables {

    /**
     * Generic method for accessing an environmental variable which has the given
     * name and will be transformed into the return type using the given conversion
     * function. The behavior in case of an error is defined by the given default
     * value and {@link Logger} objects. If a problem occurs and a {@link Logger} is
     * available, the error will be logged using the {@link Logger#error(String)}
     * method. If exceptionWhenFailing is set to {@code true} an
     * {@link IllegalStateException} is thrown. Else, if a defaultValueFactory is
     * available, a default value will be returned. Otherwise {@code null} is
     * returned.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param conversion
     *            the function which is used to convert the {@link String} of the
     *            variable value into the expected value type. It is assumed that
     *            this function will throw an exception if an error occurs.
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static <T> T getVariableValue(String name, Function<String, T> conversion,
            Supplier<T> defaultValueFactory, Logger logger, boolean exceptionWhenFailing) throws IllegalStateException {
        Map<String, String> env = System.getenv();
        String errorMsg;
        Throwable error = null;
        // If the variable is available
        if (env.containsKey(name)) {
            try {
                return conversion.apply(env.get(name));
            } catch (Throwable t) {
                errorMsg = "Error while reading the value of the variable " + name + ".";
                error = t;
                // If the logger is available, log the parsing error
                if (logger != null) {
                    logger.error(errorMsg, t);
                }
            }
        } else {
            errorMsg = "Couldn't find the expected variable " + name + " from environment.";
            if ((logger != null) && (defaultValueFactory != null)) {
                logger.error(errorMsg);
            }
        }
        if (exceptionWhenFailing) {
            if (error != null) {
                throw new IllegalStateException(errorMsg, error);
            } else {
                throw new IllegalStateException(errorMsg);
            }
        } else {
            return defaultValueFactory.get();
        }
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link String} or throws an {@link IllegalStateException} if the variable can
     * not be found or an error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @return the variable value
     * @throws IllegalStateException
     *             if the variable can not be found or an error occurs.
     */
    public static String getString(String name) throws IllegalStateException {
        return getStringValue(name, null, null, true, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link String} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static String getString(String name, String defaultValue) {
        return getStringValue(name, defaultValue, null, false, true);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link String} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static String getString(String name, Supplier<String> defaultValueFactory) {
        return getStringValue(name, defaultValueFactory, null, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link String} or throws an {@link IllegalStateException} if the variable can
     * not be found or an error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value
     * @throws IllegalStateException
     *             if the variable can not be found or an error occurs.
     */
    public static String getString(String name, Logger logger) throws IllegalStateException {
        return getStringValue(name, null, logger, true, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link String} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static String getString(String name, String defaultValue, Logger logger) {
        return getStringValue(name, defaultValue, logger, false, true);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link String} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static String getString(String name, Supplier<String> defaultValueFactory, Logger logger) {
        return getStringValue(name, defaultValueFactory, logger, false);
    }

    /**
     * Internal method defining the default value factory function before calling
     * {@link #getStringValue(String, Supplier, Logger, boolean)}.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @param hasDefaultValue
     *            flag indicating whether a default value has been provided.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static String getStringValue(String name, String defaultValue, Logger logger,
            boolean exceptionWhenFailing, boolean hasDefaultValue) throws IllegalStateException {
        return getStringValue(name, hasDefaultValue ? (() -> defaultValue) : null, logger, exceptionWhenFailing);
    }

    /**
     * Internal method defining the conversion function before calling
     * {@link #getVariableValue(String, Function, Supplier, Logger, boolean)}.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static String getStringValue(String name, Supplier<String> defaultValueFactory, Logger logger,
            boolean exceptionWhenFailing) throws IllegalStateException {
        return getVariableValue(name, (s -> s), defaultValueFactory, logger, exceptionWhenFailing);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link Model} or throws an {@link IllegalStateException} if the variable can
     * not be found or an error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @return the variable value
     * @throws IllegalStateException
     *             if the variable can not be found or an error occurs.
     */
    public static Model getModel(String name) throws IllegalStateException {
        return getModelValue(name, null, null, true, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link Model} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static Model getModel(String name, Model defaultValue) {
        return getModelValue(name, defaultValue, null, false, true);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link Model} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static Model getModel(String name, Supplier<Model> defaultValueFactory) {
        return getModelValue(name, defaultValueFactory, null, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link Model} or throws an {@link IllegalStateException} if the variable can
     * not be found or an error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value
     * @throws IllegalStateException
     *             if the variable can not be found or an error occurs.
     */
    public static Model getModel(String name, Logger logger) throws IllegalStateException {
        return getModelValue(name, null, logger, true, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link Model} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static Model getModel(String name, Model defaultValue, Logger logger) {
        return getModelValue(name, defaultValue, logger, false, true);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@link Model} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static Model getModel(String name, Supplier<Model> defaultValueFactory, Logger logger) {
        return getModelValue(name, defaultValueFactory, logger, false);
    }

    /**
     * Internal method defining the default value factory function before calling
     * {@link #getModelValue(String, Supplier, Logger, boolean)}.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @param hasDefaultValue
     *            flag indicating whether a default value has been provided.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static Model getModelValue(String name, Model defaultValue, Logger logger, boolean exceptionWhenFailing,
            boolean hasDefaultValue) throws IllegalStateException {
        return getModelValue(name, hasDefaultValue ? (() -> defaultValue) : null, logger, exceptionWhenFailing);
    }

    /**
     * Internal method defining the conversion function before calling
     * {@link #getVariableValue(String, Function, Supplier, Logger, boolean)}.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static Model getModelValue(String name, Supplier<Model> defaultValueFactory, Logger logger,
            boolean exceptionWhenFailing) throws IllegalStateException {
        return getVariableValue(name, RabbitMQUtils::readModel, defaultValueFactory, logger, exceptionWhenFailing);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code int} or throws an {@link IllegalStateException} if the variable can
     * not be found or an error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @return the variable value
     * @throws IllegalStateException
     *             if the variable can not be found or an error occurs.
     */
    public static int getInt(String name) throws IllegalStateException {
        return getIntValue(name, 0, null, true, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code int} or the default value if the variable can not be found or an error
     * occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static int getInt(String name, int defaultValue) {
        return getIntValue(name, defaultValue, null, false, true);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code int} or the default value if the variable can not be found or an error
     * occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static int getInt(String name, Supplier<Integer> defaultValueFactory) {
        return getIntValue(name, defaultValueFactory, null, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code int} or throws an {@link IllegalStateException} if the variable can
     * not be found or an error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value
     * @throws IllegalStateException
     *             if the variable can not be found or an error occurs.
     */
    public static int getInt(String name, Logger logger) throws IllegalStateException {
        return getIntValue(name, 0, logger, true, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code int} or the default value if the variable can not be found or an error
     * occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static int getInt(String name, int defaultValue, Logger logger) {
        return getIntValue(name, defaultValue, logger, false, true);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code int} or the default value if the variable can not be found or an error
     * occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static int getInt(String name, Supplier<Integer> defaultValueFactory, Logger logger) {
        return getIntValue(name, defaultValueFactory, logger, false);
    }

    /**
     * Internal method defining the default value factory function before calling
     * {@link #getIntValue(String, Supplier, Logger, boolean)}.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @param hasDefaultValue
     *            flag indicating whether a default value has been provided.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static int getIntValue(String name, int defaultValue, Logger logger, boolean exceptionWhenFailing,
            boolean hasDefaultValue) throws IllegalStateException {
        return getVariableValue(name, Integer::parseInt, hasDefaultValue ? (() -> defaultValue) : null, logger,
                exceptionWhenFailing);
    }

    /**
     * Internal method defining the conversion function before calling
     * {@link #getVariableValue(String, Function, Supplier, Logger, boolean)}.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static int getIntValue(String name, Supplier<Integer> defaultValueFactory, Logger logger,
            boolean exceptionWhenFailing) throws IllegalStateException {
        return getIntValue(name, defaultValueFactory, logger, exceptionWhenFailing);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code long} or throws an {@link IllegalStateException} if the variable can
     * not be found or an error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @return the variable value
     * @throws IllegalStateException
     *             if the variable can not be found or an error occurs.
     */
    public static long getLong(String name) throws IllegalStateException {
        return getLongValue(name, 0, null, true, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code long} or the default value if the variable can not be found or an error
     * occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static long getLong(String name, long defaultValue) {
        return getLongValue(name, defaultValue, null, false, true);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code long} or the default value if the variable can not be found or an error
     * occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static long getLong(String name, Supplier<Long> defaultValueFactory) {
        return getLongValue(name, defaultValueFactory, null, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code long} or throws an {@link IllegalStateException} if the variable can
     * not be found or an error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value
     * @throws IllegalStateException
     *             if the variable can not be found or an error occurs.
     */
    public static long getLong(String name, Logger logger) throws IllegalStateException {
        return getLongValue(name, 0, logger, true, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code long} or the default value if the variable can not be found or an error
     * occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static long getLong(String name, long defaultValue, Logger logger) {
        return getLongValue(name, defaultValue, logger, false, true);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code long} or the default value if the variable can not be found or an error
     * occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static long getLong(String name, Supplier<Long> defaultValueFactory, Logger logger) {
        return getLongValue(name, defaultValueFactory, logger, false);
    }

    /**
     * Internal method defining the default value factory function before calling
     * {@link #getVariableValue(String, Function, Supplier, Logger, boolean)}.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @param hasDefaultValue
     *            flag indicating whether a default value has been provided.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static long getLongValue(String name, long defaultValue, Logger logger, boolean exceptionWhenFailing,
            boolean hasDefaultValue) throws IllegalStateException {
        return getVariableValue(name, Long::parseLong, hasDefaultValue ? (() -> defaultValue) : null, logger,
                exceptionWhenFailing);
    }

    /**
     * Internal method defining the conversion function before calling
     * {@link #getVariableValue(String, Function, Supplier, Logger, boolean)}.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static long getLongValue(String name, Supplier<Long> defaultValueFactory, Logger logger,
            boolean exceptionWhenFailing) throws IllegalStateException {
        return getLongValue(name, defaultValueFactory, logger, exceptionWhenFailing);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code boolean} or throws an {@link IllegalStateException} if the variable
     * can not be found or an error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @return the variable value
     * @throws IllegalStateException
     *             if the variable can not be found or an error occurs.
     */
    public static boolean getBoolean(String name) throws IllegalStateException {
        return getBooleanValue(name, false, null, true, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code boolean} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static boolean getBoolean(String name, boolean defaultValue) {
        return getBooleanValue(name, defaultValue, null, false, true);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code boolean} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static boolean getBoolean(String name, Supplier<Boolean> defaultValueFactory) {
        return getBooleanValue(name, defaultValueFactory, null, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code boolean} or throws an {@link IllegalStateException} if the variable
     * can not be found or an error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value
     * @throws IllegalStateException
     *             if the variable can not be found or an error occurs.
     */
    public static boolean getBoolean(String name, Logger logger) throws IllegalStateException {
        return getBooleanValue(name, false, logger, true, false);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code boolean} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static boolean getBoolean(String name, boolean defaultValue, Logger logger) {
        return getBooleanValue(name, defaultValue, logger, false, true);
    }

    /**
     * Returns the value of the environmental variable with the given name as
     * {@code boolean} or the default value if the variable can not be found or an
     * error occurs.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @return the variable value or the default value if an error occurred and a
     *         default value is available.
     */
    public static boolean getBoolean(String name, Supplier<Boolean> defaultValueFactory, Logger logger) {
        return getBooleanValue(name, defaultValueFactory, logger, false);
    }

    /**
     * Internal method defining the default value factory function before calling
     * {@link #getBooleanValue(String, Supplier, Logger, boolean)}.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValue
     *            the default value which will be returned if the variable can not
     *            be found or if an error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @param hasDefaultValue
     *            flag indicating whether a default value has been provided.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static boolean getBooleanValue(String name, boolean defaultValue, Logger logger,
            boolean exceptionWhenFailing, boolean hasDefaultValue) throws IllegalStateException {
        return getBooleanValue(name, hasDefaultValue ? (() -> defaultValue) : null, logger, exceptionWhenFailing);
    }

    /**
     * Internal method defining the conversion function before calling
     * {@link #getVariableValue(String, Function, Supplier, Logger, boolean)}.
     * 
     * @param name
     *            name of the environmental variable which should be accessed
     * @param defaultValueFactory
     *            A factory method which can be used to generate a default value
     *            which will be returned if the variable can not be found or if an
     *            error occurs.
     * @param logger
     *            the {@link Logger} which will be used to log errors if they occur.
     * @param exceptionWhenFailing
     *            flag indicating whether an exception should be thrown if an error
     *            occurs.
     * @return the variable value converted to the expected value or the default
     *         value if an error occurred and a default value is available.
     * @throws IllegalStateException
     *             if exceptionWhenFailing is set to {@code true} and one of the
     *             following two errors occurs: 1) the variable is not available or
     *             2) the conversion function throws an exception.
     */
    protected static boolean getBooleanValue(String name, Supplier<Boolean> defaultValueFactory, Logger logger,
            boolean exceptionWhenFailing) throws IllegalStateException {
        return getVariableValue(name, (s -> {
            try {
                return Boolean.parseBoolean(s);
            } catch (Exception e) {
                try {
                    return Integer.parseInt(s) != 0;
                } catch (Exception e2) {
                    throw e;
                }
            }
        }), defaultValueFactory, logger, exceptionWhenFailing);
    }

}
