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

/*
 * Stub class - Current impl supports Jasypt decrytion only.
 * Extend this class if and when there is a need to support other encryption schemes
 */

import java.util.Optional;

public abstract class DecryptorFactory {
    public static final String DECRYPTOR_FACTORY_PROP = "decryptorFactory.name";

    private static final DecryptorFactory factory;
    private Decryptor decryptor;

    static {
        String factoryClassName = Optional.ofNullable(System.getProperty(DECRYPTOR_FACTORY_PROP))
                                          .orElseGet(JasyptDecryptorFactory.class::getName);
        try {
            factory = (DecryptorFactory) Class.forName(factoryClassName).newInstance();
        } catch (Throwable e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static DecryptorFactory getInstance() {
        return factory;
    }

    public synchronized Decryptor getDecryptor() {
        if (decryptor == null) {
            decryptor = createDecryptor();
        }
        return decryptor;
    }

    protected abstract Decryptor createDecryptor();
}
