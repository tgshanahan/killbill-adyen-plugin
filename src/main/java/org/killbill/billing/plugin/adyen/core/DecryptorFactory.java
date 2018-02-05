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

public class DecryptorFactory {

    private static final DecryptorFactory factory = new DecryptorFactory();
    private final Decryptor decryptor = new JasyptDecryptor();

    private DecryptorFactory() {};

    public static DecryptorFactory getInstance() {
        return factory;
    }

    /**
     * get the default decryptor
     * @return the default Decryptor
     */
    public Decryptor getDecryptor() {
        return decryptor;
    }
}
