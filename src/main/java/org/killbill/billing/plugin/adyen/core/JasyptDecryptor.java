/*
 * Copyright 2014-2018 Groupon, Inc
 * Copyright 2014-2018 The Billing Project, LLC
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

import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillLogService;
import org.osgi.service.log.LogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JasyptDecryptor implements Decryptor {

    private static final String JASYPT_ENCRYPTOR_PASSWORD_KEY = "JASYPT_ENCRYPTOR_PASSWORD";
    private static final String JASYPT_ENCRYPTOR_ALGORITHM_KEY = "JASYPT_ENCRYPTOR_ALGORITHM";

    private static final Logger log = LoggerFactory.getLogger(JasyptDecryptor.class);

    private final String password;
    private final String algorithm;
    private final StandardPBEStringEncryptor encryptor;

    public JasyptDecryptor() {
        this.password = System.getenv(JASYPT_ENCRYPTOR_PASSWORD_KEY);
        this.algorithm = System.getenv(JASYPT_ENCRYPTOR_ALGORITHM_KEY);
        encryptor = initializeEncryptor(password, algorithm);
    }

    public JasyptDecryptor(String password, String algorithm) {
        this.password = password;
        this.algorithm = algorithm;
        encryptor = initializeEncryptor(password, algorithm);
    }

    @Override
    public String decrypt(final String encryptedValue) {
        return encryptor.decrypt(encryptedValue);
    }

    @Override
    public void decryptProperties(Properties properties) {
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

    protected StandardPBEStringEncryptor initializeEncryptor(String password, String algorithm) {
        StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();

        if (StringUtils.isBlank(password)) {
            log.error("Required decryption property '" + JASYPT_ENCRYPTOR_PASSWORD_KEY + "' is not set");
        }
        if (StringUtils.isBlank(algorithm)) {
            log.error("Required decryption property '" + JASYPT_ENCRYPTOR_ALGORITHM_KEY + " is not set");
        }
        encryptor.setPassword(password);
        encryptor.setAlgorithm(algorithm);
        return encryptor;
    }
}
