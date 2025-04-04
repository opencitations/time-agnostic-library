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
package org.hobbit.storage.client;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFactory;
import org.apache.jena.rdf.model.Model;
import org.hobbit.core.Constants;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.core.rabbit.RabbitRpcClient;
import org.hobbit.encryption.AES;
import org.hobbit.encryption.AESException;
import org.hobbit.storage.queries.SparqlQueries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Connection;

/**
 * Simple client of the storage service implementing a synchronized
 * communication.
 *
 * @author Michael R&ouml;der (roeder@informatik.uni-leipzig.de)
 *
 */
public class StorageServiceClient implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(StorageServiceClient.class);
    /**
     * The default maximum amount of time in millisecond the client is waiting
     * for a response = {@value #DEFAULT_MAX_WAITING_TIME}ms.
     */
    private static final long DEFAULT_MAX_WAITING_TIME = 60000;

    /**
     * Creates a StorageServiceClient using the given RabbitMQ
     * {@link Connection}.
     *
     * @param connection
     *            RabbitMQ connection used for the communication
     * @return a StorageServiceClient instance
     * @throws IOException
     *             if a problem occurs during the creation of the queues or the
     *             consumer.
     */
    public static StorageServiceClient create(Connection connection) throws IOException {
        RabbitRpcClient rpcClient = RabbitRpcClient.create(connection, Constants.STORAGE_QUEUE_NAME);
        rpcClient.setMaxWaitingTime(DEFAULT_MAX_WAITING_TIME);
        return new StorageServiceClient(rpcClient);
    }

    /**
     * RPC client that is used for the communication.
     */
    private RabbitRpcClient rpcClient;

    /**
     * Constructor creating a StorageServiceClient using the given
     * {@link RabbitRpcClient}.
     *
     * @param rpcClient
     *            RPC client that is used for the communication
     */
    public StorageServiceClient(RabbitRpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    /**
     * Sets the maximum amount of time the client is waiting for a response.
     *
     * @param maxWaitingTime
     *            the maximum waiting time in milliseconds
     */
    public void setMaxWaitingTime(long maxWaitingTime) {
        this.rpcClient.setMaxWaitingTime(maxWaitingTime);
    }

    /**
     * Sends a request using rpcClient.
     *
     * If environment vars AES_PASSWORD and AES_SALT are set, the request will be encrypted.
     */
    private byte[] sendRequest(String request) {
        String AES_PASSWORD = System.getenv("AES_PASSWORD");
        String AES_SALT = System.getenv("AES_SALT");
        if(AES_PASSWORD != null && AES_SALT != null) {
            try {
                AES encryption = new AES(AES_PASSWORD, AES_SALT);
                byte[] encryptedRequest = new byte[0];
                encryptedRequest = encryption.encrypt(request);
                byte[] encryptedResponse = rpcClient.request(encryptedRequest);
                byte[] response = encryption.decrypt(encryptedResponse);
                return response;
            } catch (AESException e) {
                LOGGER.error("Encryption failed while sending request to storage service. Returning null.", e);
                return null;
            }
        } else {
            LOGGER.debug("AES_PASSWORD and AES_SALT are not set, sending unencrypted request to Rabbitmq.");
            return rpcClient.request(RabbitMQUtils.writeString(request));
        }
    }

    /**
     * Sends the given ASK query to the storage service and returns a boolean
     * value or throws an Exception if an error occurs, the service needs too
     * much time to respond or the response couldn't be parsed.
     *
     * @param query
     *            ASK query
     * @return result for the query or <code>false</code> if an error occurs
     * @throws Exception
     *             if no response has been received or the response couldn't be
     *             parsed.
     */
    public boolean sendAskQuery(String query) throws Exception {
        if (query == null) {
            throw new IllegalArgumentException("The given query is null.");
        }
        byte[] response = sendRequest(query);
        if (response != null) {
            try {
                return Boolean.parseBoolean(RabbitMQUtils.readString(response));
            } catch (Exception e) {
                throw new IOException("Couldn't parse boolean value from response. Returning false.", e);
            }
        }
        throw new IOException("Couldn't get response for query.");
    }

    /**
     * Sends the given CONSTRUCT query to the storage service and returns a
     * {@link Model} value or <code>null</code> if an error occurs, the service
     * needs too much time to respond or the response couldn't be parsed.
     *
     * @param query
     *            CONSTRUCT query
     * @return result for the query or <code>null</code>
     */
    public Model sendConstructQuery(String query) {
        if (query == null) {
            LOGGER.error("The given query is null. Returning null.");
            return null;
        }
        byte[] response = sendRequest(query);
        if (response != null) {
            try {
                return RabbitMQUtils.readModel(response);
            } catch (Exception e) {
                LOGGER.error("The response couldn't be parsed. Returning null.", e);
            }
        }
        return null;
    }

    /**
     * Sends the given DESCRIBE query to the storage service and returns a
     * {@link Model} value or <code>null</code> if an error occurs, the service
     * needs too much time to respond or the response couldn't be parsed.
     *
     * @param query
     *            DESCRIBE query
     * @return result for the query or <code>null</code>
     */
    public Model sendDescribeQuery(String query) {
        if (query == null) {
            LOGGER.error("The given query is null. Returning null.");
            return null;
        }
        byte[] response = sendRequest(query);
        if (response != null) {
            try {
                return RabbitMQUtils.readModel(response);
            } catch (Exception e) {
                LOGGER.error("The response couldn't be parsed. Returning null.", e);
            }
        }
        return null;
    }

    /**
     * Sends the given SELECT query to the storage service and returns a
     * {@link ResultSet} value or <code>null</code> if an error occurs, the
     * service needs too much time to respond or the response couldn't be
     * parsed.
     *
     * @param query
     *            SELECT query
     * @return result for the query or <code>null</code>
     */
    public ResultSet sendSelectQuery(String query) {
        if (query == null) {
            LOGGER.error("The given query is null. Returning null.");
            return null;
        }
        byte[] response = sendRequest(query);
        if (response != null) {
            try {
                ByteArrayInputStream in = new ByteArrayInputStream(response);
                ResultSet result = ResultSetFactory.fromJSON(in);
                return result;
            } catch (Exception e) {
                LOGGER.error("The response couldn't be parsed. Returning null.", e);
            }
        }
        return null;
    }

    /**
     * Sends the given UPDATE query to the storage service and returns
     * <code>true</code> if the query is successful or <code>false</code> if an
     * error occurs or the service needs too much time to respond.
     *
     * @param query
     *            UPDATE query
     * @return flag indicating whether the query has been executed successfully
     *         or not
     */
    public boolean sendUpdateQuery(String query) {
        if (query == null) {
            LOGGER.error("Can not send an update query that is null. Returning false.");
            return false;
        }
        byte[] response = sendRequest(query);
        return (response != null) && (response.length > 0);
    }

    /**
     * Inserts the given model into the storage and returns <code>true</code> if
     * the query is successful or <code>false</code> if an error occurs or the
     * service needs too much time to respond.
     *
     * @param model
     *            RDF model containing triples that should be inserted
     * @param graphURI
     *            URI of the graph in which the model should be inserted
     * @return flag indicating whether the query has been executed successfully
     *         or not
     */
    public boolean sendInsertQuery(Model model, String graphURI) {
        if (model == null) {
            LOGGER.error("Can not store a model that is null. Returning false.");
            return false;
        }
        if (graphURI == null) {
            LOGGER.error("Can not store a model without a graph URI. Returning false.");
            return false;
        }
        String queries[] = SparqlQueries.getUpdateQueriesFromDiff(null, model, graphURI);
        boolean success = true;
        for (int i = 0; i < queries.length; ++i) {
            success &= sendUpdateQuery(queries[i]);
        }
        return success;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Closes the internal {@link #rpcClient} instance.
     * </p>
     */
    @Override
    public void close() throws IOException {
        IOUtils.closeQuietly(rpcClient);
    }

}
