package org.killbill.billing.plugin.adyen.core;

import org.killbill.billing.osgi.api.Healthcheck.HealthStatus;
import org.killbill.billing.plugin.adyen.client.AdyenConfigProperties;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class AdyenHealthCheckTest {
    private AdyenConfigPropertiesConfigurationHandler adyenConfigPropertiesConfigurationHandler;

    private void initMockedAdyenConfigProperties(boolean shouldPerformHealthCheck) {
        AdyenConfigProperties adyenConfigProperties = Mockito.mock(AdyenConfigProperties.class);
        when(adyenConfigProperties.getShouldPerformHealthCheck()).thenReturn(shouldPerformHealthCheck);

        adyenConfigPropertiesConfigurationHandler = Mockito.mock(AdyenConfigPropertiesConfigurationHandler.class);
        when(adyenConfigPropertiesConfigurationHandler.getConfigurable(anyObject())).thenReturn(adyenConfigProperties);
    }

    @Test
    public void testShouldPerformHealthCheck() {
        // given
        initMockedAdyenConfigProperties(true);
        AdyenHealthcheck adyenHealthCheck = new AdyenHealthcheck(adyenConfigPropertiesConfigurationHandler);

        // when
        HealthStatus healthStatus = adyenHealthCheck.getHealthStatus(null, null);

        // then

        // I don't want it to make an HTTP call so we leave the URL empty which will generate
        // a MalformedURLException that we detect below and implies we attempted a health check
        assertEquals(false, healthStatus.isHealthy());
        assertEquals("null java.net.MalformedURLException", healthStatus.getDetails().get("message"));
    }

    @Test
    public void testNotShouldPerformHealthCheck() {
        // given
        initMockedAdyenConfigProperties(false);
        AdyenHealthcheck adyenHealthCheck = new AdyenHealthcheck(adyenConfigPropertiesConfigurationHandler);

        // when
        HealthStatus healthStatus = adyenHealthCheck.getHealthStatus(null, null);

        // then
        assertEquals(true, healthStatus.isHealthy());
        assertEquals("Health check skipped per configuration, assumed healthy.", healthStatus.getDetails().get("message"));
    }
}
