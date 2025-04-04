package org.hobbit.core.rabbit;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;

/**
 * A class that offers simple utility methods that hide some steps that are
 * necessary to transform java objects into JSON containing byte arrays and vice
 * versa.
 * 
 * @author Michael R&ouml;der (michael.roeder@uni-paderborn.de)
 *
 */
public class GsonUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(GsonUtils.class);

    /**
     * Serialize a Java object as JSON using the given {@link Gson} instance and the
     * {@link RabbitMQUtils} class.
     * 
     * @param <T>    The class of the given object
     * @param gson   The Gson instance used for the serialization as JSON
     * @param object The object that should be serialized
     * @return The serialized object as JSON in a byte representation or null if the
     *         given object was null
     */
    public static <T> byte[] serializeObjectWithGson(Gson gson, T object) {
        if (object != null) {
            return RabbitMQUtils.writeString(gson.toJson(object));
        }
        return null;
    }

    /**
     * Deserialize a Java data object that was received as JSON with a command.
     * First, the given byte array will be transformed into a String using the
     * {@link RabbitMQUtils} class, before it will be deserialized using the given
     * {@link Gson} object.
     * 
     * @param <T>            The class that the data object should have.
     * @param gson           The Gson instance used to deserialize the JSON object.
     * @param data           The byte array that has been received.
     * @param clazz          The class that the data object should have.
     * @param containsLength This flag indicates whether the given byte array
     *                       contains the data with a preceeded int value containing
     *                       the length of the next piece of data
     * @return The deserialized object or null if an error occurred
     */
    public static <T> T deserializeObjectWithGson(Gson gson, byte[] data, Class<? extends T> clazz,
            boolean containsLength) {
        if (data != null) {
            if (containsLength) {
                return deserializeObjectWithGson(gson, ByteBuffer.wrap(data), clazz);
            } else {
                String dataString = RabbitMQUtils.readString(data);
                return deserializeObjectWithGson(gson, dataString, clazz);
            }
        }
        return null;
    }

    /**
     * Deserialize a Java data object that was received as JSON with a command.
     * First, the given byte array will be transformed into a String using the
     * {@link RabbitMQUtils} class, before it will be deserialized using the given
     * {@link Gson} object.
     * 
     * @param <T>   The class that the data object should have.
     * @param gson  The Gson instance used to deserialize the JSON object.
     * @param data  A ByteBuffer containing the data and the length of the data.
     * @param clazz The class that the data object should have.
     * @return The deserialized object or null if an error occurred
     */
    public static <T> T deserializeObjectWithGson(Gson gson, ByteBuffer data, Class<? extends T> clazz) {
        if (data != null) {
            String dataString = RabbitMQUtils.readString(data);
            return deserializeObjectWithGson(gson, dataString, clazz);
        }
        return null;
    }

    protected static <T> T deserializeObjectWithGson(Gson gson, String dataString, Class<? extends T> clazz) {
        try {
            return gson.fromJson(dataString, clazz);
        } catch (Exception e) {
            LOGGER.error("Error while parsing JSON data form String \"" + dataString + "\". Returning null.", e);
            return null;
        }
    }
}
