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
import java.util.Enumeration;
import java.util.Properties;
import java.util.UUID;

import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillLogService;
import org.killbill.billing.plugin.adyen.client.AdyenConfigProperties;
import org.killbill.billing.plugin.api.notification.PluginTenantConfigurableConfigurationHandler;
import org.osgi.service.log.LogService;

public class AdyenConfigPropertiesConfigurationHandler extends PluginTenantConfigurableConfigurationHandler<AdyenConfigProperties> {

    private final String configKeyName;
    private final String region;
    private final OSGIKillbillLogService osgiKillbillLogService;

    private static final String ENC_PREFIX = "ENC(";
    private static final String ENC_SUFFIX = ")";

    public AdyenConfigPropertiesConfigurationHandler(final String pluginName,
                                                     final OSGIKillbillAPI osgiKillbillAPI,
                                                     final OSGIKillbillLogService osgiKillbillLogService,
                                                     final String region) {
        super(pluginName, osgiKillbillAPI, osgiKillbillLogService);
        this.configKeyName = "PLUGIN_CONFIG_" + pluginName;
        this.region = region;
        this.osgiKillbillLogService = osgiKillbillLogService;
    }

    @Override
    protected AdyenConfigProperties createConfigurable(final Properties properties) {
        return new AdyenConfigProperties(properties, region);
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
            decryptProperties(properties);
            return properties;
        } catch (final IOException e) {
            osgiKillbillLogService.log(LogService.LOG_WARNING, "Exception while loading properties for key " + configKeyName, e);
            return null;
        }
    }

    protected void decryptProperties(Properties properties) {
        Decryptor encryptor = new JasyptDecryptor(osgiKillbillLogService);
        Enumeration keys = properties.keys();
        while (keys.hasMoreElements()) {
            final String key = (String) keys.nextElement();
            final String value = (String) properties.get(key);
            String decryptableValue = StringUtils.substringBetween(value, ENC_PREFIX, ENC_SUFFIX);
            if (decryptableValue != null) {
                properties.setProperty(key, encryptor.decrypt(decryptableValue));
            }
        }
    }

}
