/*
 * Copyright 2014-2016 Groupon, Inc
 * Copyright 2014-2016 The Billing Project, LLC
 *
 * The Billing Project licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.killbill.billing.plugin.adyen.core;

import java.io.IOException;
import java.io.StringReader;
import java.util.Properties;
import java.util.UUID;

import javax.annotation.Nullable;

import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillLogService;
import org.killbill.billing.plugin.adyen.client.AdyenConfigProperties;
import org.killbill.billing.plugin.adyen.client.AdyenPaymentPortRegistry;
import org.killbill.billing.plugin.adyen.client.PaymentPortRegistry;
import org.killbill.billing.plugin.adyen.client.jaxws.HttpHeaderInterceptor;
import org.killbill.billing.plugin.adyen.client.jaxws.LoggingInInterceptor;
import org.killbill.billing.plugin.adyen.client.jaxws.LoggingOutInterceptor;
import org.killbill.billing.plugin.adyen.client.payment.builder.AdyenRequestFactory;
import org.killbill.billing.plugin.adyen.client.payment.converter.PaymentInfoConverterManagement;
import org.killbill.billing.plugin.adyen.client.payment.converter.impl.PaymentInfoConverterService;
import org.killbill.billing.plugin.adyen.client.payment.service.AdyenPaymentRequestSender;
import org.killbill.billing.plugin.adyen.client.payment.service.AdyenPaymentServiceProviderPort;
import org.killbill.billing.plugin.adyen.client.payment.service.Signer;
import org.killbill.billing.plugin.api.notification.PluginTenantConfigurableConfigurationHandler;
import org.osgi.service.log.LogService;

public class AdyenConfigurationHandler extends PluginTenantConfigurableConfigurationHandler<AdyenPaymentServiceProviderPort> {

    private final String region;
    private final OSGIKillbillLogService osgiKillbillLogService;
    private final Decryptor decryptor;

    public AdyenConfigurationHandler(final String pluginName,
                                     final OSGIKillbillAPI osgiKillbillAPI,
                                     final OSGIKillbillLogService osgiKillbillLogService,
                                     final String region) {
        super(pluginName, osgiKillbillAPI, osgiKillbillLogService);
        this.region = region;
        this.osgiKillbillLogService = osgiKillbillLogService;
        this.decryptor = DecryptorFactory.getInstance().getDecryptor();
    }

    @Override
    protected AdyenPaymentServiceProviderPort createConfigurable(final Properties properties) {
        final AdyenConfigProperties adyenConfigProperties = new AdyenConfigProperties(properties, region);
        return initializeAdyenClient(adyenConfigProperties);
    }

    private AdyenPaymentServiceProviderPort initializeAdyenClient(final AdyenConfigProperties adyenConfigProperties) {
        final PaymentInfoConverterManagement paymentInfoConverterManagement = new PaymentInfoConverterService();

        final Signer signer = new Signer();
        final AdyenRequestFactory adyenRequestFactory = new AdyenRequestFactory(paymentInfoConverterManagement, adyenConfigProperties, signer);

        final LoggingInInterceptor loggingInInterceptor = new LoggingInInterceptor();
        final LoggingOutInterceptor loggingOutInterceptor = new LoggingOutInterceptor();
        final HttpHeaderInterceptor httpHeaderInterceptor = new HttpHeaderInterceptor();
        final PaymentPortRegistry adyenPaymentPortRegistry = new AdyenPaymentPortRegistry(adyenConfigProperties, loggingInInterceptor, loggingOutInterceptor, httpHeaderInterceptor);
        final AdyenPaymentRequestSender adyenPaymentRequestSender = new AdyenPaymentRequestSender(adyenPaymentPortRegistry);

        return new AdyenPaymentServiceProviderPort(adyenRequestFactory, adyenPaymentRequestSender);
    }


    @Override
    protected Properties getTenantConfigurationAsProperties(@Nullable final UUID kbTenantId) {
        final String tenantConfigurationAsString = getTenantConfigurationAsString(kbTenantId);
        if (tenantConfigurationAsString == null) {
            return null;
        }

        final Properties properties = new Properties();
        try {
            properties.load(new StringReader(tenantConfigurationAsString));
            decryptor.decryptProperties(properties);
            return properties;
        } catch (final IOException e) {
            osgiKillbillLogService.log(LogService.LOG_WARNING, "Exception while loading properties for tenant " + kbTenantId, e);
            return null;
        }
    }

}
