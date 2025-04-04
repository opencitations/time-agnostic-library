package org.hobbit.utils.kubernetes;

import org.hobbit.utils.config.HobbitConfiguration;
import org.junit.Test;
import static org.junit.Assert.*;

public class KubeHelperTest {

    @Test
    public void testGetPodIP() {
        String podIP = KubeHelper.getPodIP();
        assertNotNull(podIP);
        assertFalse(podIP.isEmpty());
    }

    @Test
    public void testGetDnsFriendlyIP() {
        // Ensure getPodIP() returns something valid before testing
        String podIP = KubeHelper.getPodIP();
        HobbitConfiguration HC = new HobbitConfiguration();
        String namespace = HC.getString("POD_NAMESPACE","default");
        if (podIP != null) {
            String dnsFriendlyIP = KubeHelper.getDnsFriendlyIP();
            assertTrue(dnsFriendlyIP.contains(namespace));  // Must include the namespace
            assertTrue(dnsFriendlyIP.contains("-"));  // IP should be hyphenated
            assertTrue(dnsFriendlyIP.contains(".pod.cluster.local"));
        } else {
            // If getPodIP() fails, the method should throw an exception
            try {
                KubeHelper.getDnsFriendlyIP();
                fail("Expected IllegalStateException due to unknown IP");
            } catch (IllegalStateException e) {
                // Expected behavior
            }
        }
    }
}
