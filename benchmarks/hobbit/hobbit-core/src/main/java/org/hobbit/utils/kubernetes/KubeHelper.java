package org.hobbit.utils.kubernetes;

import org.hobbit.utils.config.HobbitConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class KubeHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(KubeHelper.class);
    private static final String DOMAIN_SUFFIX = ".pod.cluster.local";
    private static final HobbitConfiguration HC = new HobbitConfiguration();
    /**
     * Gets a DNS-friendly IP for the pod.
     *
     * @return DNS-friendly hostname (or IP address if not available).
     */
    public static String getDnsFriendlyIP() {
        String namespace = HC.getString("POD_NAMESPACE","default");

        if(namespace.equals("default") ) {
            LOGGER.warn("No pod namespace found in environment variable POD_NAMESPACE use default");
        }

        String ip = getPodIP();

        String valueToReturn = formatDnsFriendlyIp(ip, namespace);
        LOGGER.info("dns friendly version for {} in namespace {} is {}", ip, namespace, valueToReturn);
        return valueToReturn;
    }

    /**
     * Retrieves the pod's IP address.
     *
     * @return The IP address of the pod, or "unknown" if retrieval fails.
     */
    public static String getPodIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.error("Failed to retrieve pod IP", e);
            throw new IllegalStateException("Failed to retrieve pod IP", e);
        }
    }


    /**
     * Formats the IP into a DNS-friendly format for Kubernetes.
     *
     * @param ip        The pod's IP address.
     * @param namespace The namespace in which the pod is running.
     * @return A DNS-friendly hostname.
     */
    private static String formatDnsFriendlyIp(String ip, String namespace) {
        return String.format("%s.%s%s", ip.replace(".", "-"), namespace, DOMAIN_SUFFIX);
    }
}
