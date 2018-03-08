/*
 * Copyright 2016 Groupon, Inc
 * Copyright 2016 The Billing Project, LLC
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

package org.killbill.billing.plugin.adyen.client.model;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPaymentServiceProviderResult {

    @Test(groups = "fast")
    public void testLookups() throws Exception {
        // API
        Assert.assertEquals(PaymentServiceProviderResult.getPaymentResultForId("Authorised"), PaymentServiceProviderResult.AUTHORISED);
        Assert.assertEquals(PaymentServiceProviderResult.getPaymentResultForId("[refund-received]"), PaymentServiceProviderResult.RECEIVED);
        Assert.assertEquals(PaymentServiceProviderResult.getPaymentResultForId("[error]"), PaymentServiceProviderResult.ERROR);
        Assert.assertEquals(PaymentServiceProviderResult.getPaymentResultForId("Error"), PaymentServiceProviderResult.ERROR);

        // HPP
        Assert.assertEquals(PaymentServiceProviderResult.getPaymentResultForId("AUTHORISED"), PaymentServiceProviderResult.AUTHORISED);
        Assert.assertEquals(PaymentServiceProviderResult.getPaymentResultForId("REFUSED"), PaymentServiceProviderResult.REFUSED);
        Assert.assertEquals(PaymentServiceProviderResult.getPaymentResultForId("CANCELLED"), PaymentServiceProviderResult.CANCELLED);
        Assert.assertEquals(PaymentServiceProviderResult.getPaymentResultForId("PENDING"), PaymentServiceProviderResult.PENDING);
        Assert.assertEquals(PaymentServiceProviderResult.getPaymentResultForId("ERROR"), PaymentServiceProviderResult.ERROR);

        Assert.assertEquals(PaymentServiceProviderResult.getPaymentResultForId("[refund-received]   "), PaymentServiceProviderResult.RECEIVED);

    }
}
