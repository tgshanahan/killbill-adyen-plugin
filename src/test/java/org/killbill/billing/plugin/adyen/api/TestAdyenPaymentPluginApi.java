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

package org.killbill.billing.plugin.adyen.api;

import java.math.BigDecimal;
import java.net.URL;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.joda.time.Period;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.killbill.adyen.recurring.RecurringDetail;
import org.killbill.billing.account.api.Account;
import org.killbill.billing.account.api.AccountApiException;
import org.killbill.billing.payment.api.Payment;
import org.killbill.billing.payment.api.PaymentApiException;
import org.killbill.billing.payment.api.PaymentMethodPlugin;
import org.killbill.billing.payment.api.PaymentTransaction;
import org.killbill.billing.payment.api.PluginProperty;
import org.killbill.billing.payment.api.TransactionStatus;
import org.killbill.billing.payment.api.TransactionType;
import org.killbill.billing.payment.plugin.api.PaymentMethodInfoPlugin;
import org.killbill.billing.payment.plugin.api.PaymentPluginApiException;
import org.killbill.billing.payment.plugin.api.PaymentPluginStatus;
import org.killbill.billing.payment.plugin.api.PaymentTransactionInfoPlugin;
import org.killbill.billing.plugin.TestUtils;
import org.killbill.billing.plugin.adyen.dao.AdyenDao;
import org.killbill.billing.plugin.adyen.dao.gen.tables.records.AdyenPaymentMethodsRecord;
import org.killbill.billing.plugin.api.PluginProperties;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.ning.http.util.UTF8UrlEncoder;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.jayway.restassured.http.ContentType;

import javax.xml.bind.DatatypeConverter;

import static com.jayway.restassured.RestAssured.given;
import static org.killbill.billing.plugin.adyen.api.AdyenPaymentPluginApi.*;
import static org.killbill.billing.plugin.api.payment.PluginPaymentPluginApi.PROPERTY_COUNTRY;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAdyenPaymentPluginApi extends TestAdyenPaymentPluginApiBase {

    private static final long SLEEP_IN_MILLIS_FOR_RECURRING_DETAIL = 3000L; // 3 Seconds
    private static final int HTTP_200_OK = 200;
    private static final int HTTP_PORT = 80;
    private static final int HTTPS_PORT = 443;
    private static final Map<TransactionType, String> TRANSACTION_TYPE_TO_EVENT_CODE = ImmutableMap.<TransactionType, String>builder().put(TransactionType.VOID, "CANCELLATION")
            .put(TransactionType.REFUND, "REFUND")
            .put(TransactionType.CAPTURE, "CAPTURE")
            .put(TransactionType.CREDIT, "REFUND_WITH_DATA")
            .put(TransactionType.CHARGEBACK, "CHARGEBACK")
            .put(TransactionType.AUTHORIZE, "AUTHORISATION")
            .put(TransactionType.PURCHASE, "AUTHORISATION")
            .build();
    private static final Map<String, String> three3DSbrowserInfo = ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_USER_AGENT, "Java/1.8")
            .put(AdyenPaymentPluginApi.PROPERTY_ACCEPT_HEADER, "application/json")
            .put(AdyenPaymentPluginApi.PROPERTY_BROWSER_LANGUAGE, "de")
            .put(AdyenPaymentPluginApi.PROPERTY_COLOR_DEPTH, "32")
            .put(AdyenPaymentPluginApi.PROPERTY_SCREEN_HEIGHT, "1125")
            .put(AdyenPaymentPluginApi.PROPERTY_SCREEN_WIDTH, "2436")
            .put(AdyenPaymentPluginApi.PROPERTY_BROWSER_TIME_ZONE_OFFSET, "-420")
            .put(AdyenPaymentPluginApi.PROPERTY_JAVA_ENABLED, "false")
            .put(AdyenPaymentPluginApi.PROPERTY_JAVA_SCRIPT_ENABLED, "true")
            .build();
    private final Iterable<PluginProperty> propertiesWithCCInfo = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_TYPE, CC_TYPE)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Dupont")
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_NUMBER)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .build());
    private final Iterable<PluginProperty> propertiesWith3DSInfo = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_TYPE, CC_TYPE)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Montblanc")
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_3DS_NUMBER)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_3DS_VERIFICATION_VALUE)
            .put(AdyenPaymentPluginApi.PROPERTY_USER_AGENT, "Java/1.8")
            .put(AdyenPaymentPluginApi.PROPERTY_ACCEPT_HEADER, "application/json")
            .put(AdyenPaymentPluginApi.PROPERTY_TERM_URL, "dummy://url")
            .put(AdyenPaymentPluginApi.PROPERTY_THREE_D_THRESHOLD, "25000")
            .build());
    private final Iterable<PluginProperty> propertiesFor3DS2IdentifyShopper = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_TYPE, CC_TYPE)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Montblanc")
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_3DS2_NUMBER_IDENTIFY_SHOPPER)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_3DS2_VERIFICATION_VALUE)
            .put(AdyenPaymentPluginApi.PROPERTY_NOTIFICATION_URL, "https://example.com")
            .putAll(three3DSbrowserInfo)
            .build());
    private final Iterable<PluginProperty> propertiesFor3DS2IdentifyShopperNoCVC = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_TYPE, CC_TYPE)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Montblanc")
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_3DS2_NUMBER_IDENTIFY_SHOPPER)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .put(AdyenPaymentPluginApi.PROPERTY_NOTIFICATION_URL, "https://example.com")
            .put("additionalDataItem.0.key", "allow3ds2")
            .put("additionalDataItem.0.value", "true")
            .put("additionalDataItem.1.key", "executeThreeD")
            .put("additionalDataItem.1.value", "true")
            .putAll(three3DSbrowserInfo)
.build());
    private final Iterable<PluginProperty> propertiesForCustomerSupportMotoPayment = PluginProperties.buildPluginProperties(
            ImmutableMap.<String, String>builder()
                    .put(AdyenPaymentPluginApi.PROPERTY_CC_TYPE, CC_TYPE)
                    .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Montblanc")
                    .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_3DS2_NUMBER_IDENTIFY_SHOPPER)
                    .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
                    .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
                    .put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_3DS2_VERIFICATION_VALUE)
                    .put(AdyenPaymentPluginApi.PROPERTY_CUSTOMER_SUPPORT_REQUEST, "true")
                    .build());
    private final Iterable<PluginProperty> propertiesFor3DS2ChallengeShopper = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_TYPE, CC_TYPE)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Montblanc")
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_3DS2_NUMBER_CHALLENGE_SHOPPER)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_3DS2_VERIFICATION_VALUE)
            .put(AdyenPaymentPluginApi.PROPERTY_NOTIFICATION_URL, "https://example.com")
            .putAll(three3DSbrowserInfo)
            .build());
    private final Iterable<PluginProperty> propertiesWithGooglePay = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Googlepaytester")
            .put(AdyenPaymentPluginApi.PROPERTY_SELECTED_BRAND, "paywithgoogle")
            .put(AdyenPaymentPluginApi.PROPERTY_MPI_DATA_CAVV, MPI_DATA_CAVV)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_NUMBER_VISA)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .build());
    private final Iterable<PluginProperty> propertiesWithApplePay = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Applepaytester")
            .put(AdyenPaymentPluginApi.PROPERTY_SELECTED_BRAND, "applepay")
            .put(AdyenPaymentPluginApi.PROPERTY_MPI_DATA_CAVV, MPI_DATA_CAVV)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_NUMBER_VISA)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .build());
    private final Iterable<PluginProperty> propertiesWithApplePayRecurring = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Applepaytester")
            .put(AdyenPaymentPluginApi.PROPERTY_SELECTED_BRAND, "applepay")
            .put(AdyenPaymentPluginApi.PROPERTY_MPI_DATA_CAVV, MPI_DATA_CAVV)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_NUMBER_VISA)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .put(AdyenPaymentPluginApi.PROPERTY_RECURRING_TYPE, "RECURRING")
            .build());
    private final Iterable<PluginProperty> propertiesWithAVSInfo = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_TYPE, CC_TYPE)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Avschecker")
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_AVS_NUMBER)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_CVV_VERIFICATION_VALUE)
            .put(AdyenPaymentPluginApi.PROPERTY_ADDRESS1, "1600 Pennsylvania Ave NW")
            .put(AdyenPaymentPluginApi.PROPERTY_ADDRESS2, "")
            .put(AdyenPaymentPluginApi.PROPERTY_CITY, "Washington")
            .put(AdyenPaymentPluginApi.PROPERTY_STATE, "DC")
            .put(AdyenPaymentPluginApi.PROPERTY_ZIP, "20500")
            .put(AdyenPaymentPluginApi.PROPERTY_COUNTRY, DEFAULT_COUNTRY)
            .put(AdyenPaymentPluginApi.PROPERTY_USER_AGENT, "Java/1.8")
            .put(AdyenPaymentPluginApi.PROPERTY_ACCEPT_HEADER, "application/json")
            .put(AdyenPaymentPluginApi.PROPERTY_TERM_URL, "dummy://url")
            .build());
    private final Iterable<PluginProperty> propertiesWithBadAVSInfo = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_TYPE, CC_TYPE)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Avschecker")
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_AVS_NUMBER)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_CVV_VERIFICATION_VALUE)
            .put(AdyenPaymentPluginApi.PROPERTY_ADDRESS1, "1600 Pennsylvania Ave NW")
            .put(AdyenPaymentPluginApi.PROPERTY_ADDRESS2, "")
            .put(AdyenPaymentPluginApi.PROPERTY_CITY, "Washington")
            .put(AdyenPaymentPluginApi.PROPERTY_STATE, "DC")
            .put(AdyenPaymentPluginApi.PROPERTY_ZIP, "20501")  // zip is wrong
            .put(AdyenPaymentPluginApi.PROPERTY_COUNTRY, DEFAULT_COUNTRY)
            .put(AdyenPaymentPluginApi.PROPERTY_USER_AGENT, "Java/1.8")
            .put(AdyenPaymentPluginApi.PROPERTY_ACCEPT_HEADER, "application/json")
            .put(AdyenPaymentPluginApi.PROPERTY_TERM_URL, "dummy://url")
            .build());

    private final Iterable<PluginProperty> propertiesWithZipCodeOnlyAVSInfo = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
            .put(AdyenPaymentPluginApi.PROPERTY_CC_TYPE, CC_TYPE)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_LAST_NAME, "Avschecker")
            .put(AdyenPaymentPluginApi.PROPERTY_CC_NUMBER, CC_AVS_NUMBER)
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH, String.valueOf(CC_EXPIRATION_MONTH))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR, String.valueOf(CC_EXPIRATION_YEAR))
            .put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_CVV_VERIFICATION_VALUE)
            .put(AdyenPaymentPluginApi.PROPERTY_ZIP, "20500") // only sending the zip code
            .put(AdyenPaymentPluginApi.PROPERTY_COUNTRY, DEFAULT_COUNTRY)
            .put(AdyenPaymentPluginApi.PROPERTY_USER_AGENT, "Java/1.8")
            .put(AdyenPaymentPluginApi.PROPERTY_ACCEPT_HEADER, "application/json")
            .put(AdyenPaymentPluginApi.PROPERTY_TERM_URL, "dummy://url")
            .build());
    private Map<String, String> propertiesForRecurring;

    @Override
    @BeforeMethod(groups = "integration")
    public void setUpRemote() throws Exception {
        super.setUpRemote();

        propertiesForRecurring = ImmutableMap.of(AdyenPaymentPluginApi.PROPERTY_CUSTOMER_ID, UUID.randomUUID().toString(),
                                                 AdyenPaymentPluginApi.PROPERTY_EMAIL, UUID.randomUUID().toString() + "@example.com");

        assertEquals(adyenPaymentPluginApi.getPaymentMethods(account.getId(), false, ImmutableList.<PluginProperty>of(), context).size(), 0);
        assertEquals(adyenPaymentPluginApi.getPaymentMethods(account.getId(), true, ImmutableList.<PluginProperty>of(), context).size(), 0);
    }

    @Test(groups = "integration")
    public void testPaymentMethodManagement() throws Exception {
        final Iterable<PluginProperty> propertiesForAddPaymentMethod = PluginProperties.buildPluginProperties(propertiesForRecurring);
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesForAddPaymentMethod, context);

        // Verify the payment method is empty
        final List<PaymentMethodInfoPlugin> initialPaymentMethods = adyenPaymentPluginApi.getPaymentMethods(account.getId(), false, ImmutableList.<PluginProperty>of(), context);
        assertEquals(initialPaymentMethods.size(), 1);
        assertEquals(initialPaymentMethods.get(0).getAccountId(), account.getId());
        assertEquals(initialPaymentMethods.get(0).getPaymentMethodId(), account.getPaymentMethodId());
        assertNull(initialPaymentMethods.get(0).getExternalPaymentMethodId());
        assertEquals(adyenPaymentPluginApi.getPaymentMethods(account.getId(), true, ImmutableList.<PluginProperty>of(), context), initialPaymentMethods);

        // Create the recurring contract
        final Iterable<PluginProperty> propertiesWithCCForRecurring = PluginProperties.merge(ImmutableMap.<String, String>builder()
                                                                                                     .put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE)
                                                                                                     .put(AdyenPaymentPluginApi.PROPERTY_RECURRING_TYPE, "RECURRING")
                                                                                                     .build(),
                                                                                             propertiesWithCCInfo);
        final Payment payment = doAuthorize(BigDecimal.TEN, propertiesWithCCForRecurring);
        doCapture(payment, BigDecimal.TEN);
        doRefund(payment, BigDecimal.TEN);

        // Sleep a few seconds to give Adyen's Test System time to process and create the RecurringDetails
        Thread.sleep(SLEEP_IN_MILLIS_FOR_RECURRING_DETAIL);

        // No change, unless refreshFromGateway is set
        assertEquals(adyenPaymentPluginApi.getPaymentMethods(account.getId(), false, ImmutableList.<PluginProperty>of(), context), initialPaymentMethods);
        final List<PaymentMethodInfoPlugin> subsequentPaymentMethods = adyenPaymentPluginApi.getPaymentMethods(account.getId(), true, ImmutableList.<PluginProperty>of(), context);
        assertEquals(subsequentPaymentMethods.size(), 1);
        assertEquals(subsequentPaymentMethods.get(0).getAccountId(), account.getId());
        assertEquals(subsequentPaymentMethods.get(0).getPaymentMethodId(), account.getPaymentMethodId());
        // This points to the recurringDetailReference
        assertNotNull(subsequentPaymentMethods.get(0).getExternalPaymentMethodId());

        // Verify the token can be used for recurring payments
        final Payment payment2 = doAuthorize(BigDecimal.TEN, ImmutableList.<PluginProperty>of());
        doCapture(payment2, BigDecimal.TEN);
        doRefund(payment2, BigDecimal.TEN);

        // Verify the token can be used for an auto-capture recurring payment
        final Payment payment3 = doPurchase(BigDecimal.ONE, ImmutableList.<PluginProperty>of());
        doRefund(payment3, BigDecimal.ONE);

        // Verify the token can be used for another recurring payment
        final Payment payment4 = doAuthorize(BigDecimal.ONE, ImmutableList.<PluginProperty>of());
        doCapture(payment4, BigDecimal.ONE);
        doRefund(payment4, BigDecimal.ONE);

        adyenPaymentPluginApi.deletePaymentMethod(account.getId(), account.getPaymentMethodId(), ImmutableList.<PluginProperty>of(), context);

        assertEquals(adyenPaymentPluginApi.getPaymentMethods(account.getId(), false, ImmutableList.<PluginProperty>of(), context).size(), 0);
        assertEquals(adyenPaymentPluginApi.getPaymentMethods(account.getId(), true, ImmutableList.<PluginProperty>of(), context).size(), 0);
    }

    @Test(groups = "integration")
    public void testUnknownPayment() throws Exception {
        assertTrue(adyenPaymentPluginApi.getPaymentInfo(account.getId(),
                                                        UUID.randomUUID(),
                                                        ImmutableList.<PluginProperty>of(),
                                                        context).isEmpty());
    }

    @Test(groups = "integration")
    public void testAuthorizeAndCaptureSkipGw() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCInfo, context);

        final Payment payment = doAuthorize(BigDecimal.TEN, PluginProperties.buildPluginProperties(ImmutableMap.<String, String>of(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE, "skip_gw", "true")));
        doCapture(payment, new BigDecimal("5"), ImmutableList.<PluginProperty>of(new PluginProperty("skip_gw", "true", false)));
    }

    @Test(groups = "integration")
    public void testAuthorizeAndMultipleCaptures() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCInfo, context);

        final Payment payment = doAuthorize(BigDecimal.TEN, PluginProperties.buildPluginProperties(ImmutableMap.<String, String>of(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE)));
        doCapture(payment, new BigDecimal("5"));
        doCapture(payment, new BigDecimal("5"), ImmutableList.<PluginProperty>of(new PluginProperty(PROPERTY_COUNTRY, "bogus", false)));
    }

    @Test(groups = "integration")
    public void testAuthorizeAndVoid() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCInfo, context);

        final Payment payment = doAuthorize(BigDecimal.TEN, PluginProperties.buildPluginProperties(ImmutableMap.<String, String>of(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE)));
        doVoid(payment);
    }

    @Test(groups = "integration")
    public void testAuthorizeOddCountryCodes() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCInfo, context);

        for (final String countryCode : ImmutableList.<String>of("GB", "UK", "QC", "CA")) {
            doAuthorize(BigDecimal.TEN, PluginProperties.buildPluginProperties(ImmutableMap.<String, String>of(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE,
                                                                                                               AdyenPaymentPluginApi.PROPERTY_COUNTRY, countryCode)));
        }
    }

    @Test(groups = "integration")
    public void testPurchaseAndRefund() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCInfo, context);

        final Payment payment = doPurchase(BigDecimal.TEN, PluginProperties.buildPluginProperties(ImmutableMap.<String, String>of(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE)));
        doRefund(payment, BigDecimal.TEN);
    }

    // Disabled by default since Card fund transfer (CFT) isn't enabled automatically on the sandbox
    @Test(groups = "integration", enabled = false)
    public void testCredit() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCInfo, context);

        doCredit(BigDecimal.TEN, PluginProperties.buildPluginProperties(ImmutableMap.<String, String>of(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE)));
    }

    @Test(groups = "integration")
    public void testAuthorizeAndMultipleCapturesSepaDirectDebit() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenPaymentMethodPluginSepaDirectDebit(), true, ImmutableList.<PluginProperty>of(), context);

        final Payment payment = doAuthorize(BigDecimal.TEN);
        doCapture(payment, new BigDecimal("5"));
        doCapture(payment, new BigDecimal("5"));
    }

    @Test(groups = "integration")
    public void testAuthorizeAndMultipleCapturesELV() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenPaymentMethodPluginELV(), true, ImmutableList.<PluginProperty>of(), context);

        final Payment payment = doAuthorize(BigDecimal.TEN);
        doCapture(payment, new BigDecimal("5"));
        doCapture(payment, new BigDecimal("5"));
    }

    @Test(groups = "integration")
    public void testAuthorizeFailingInvalidCVV() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCInfo, context);

        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);
        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("1000"), account.getCurrency());

        final PaymentTransactionInfoPlugin authorizationInfoPlugin = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                            payment.getId(),
                                                                                                            authorizationTransaction.getId(),
                                                                                                            account.getPaymentMethodId(),
                                                                                                            authorizationTransaction.getAmount(),
                                                                                                            authorizationTransaction.getCurrency(),
                                                                                                            PluginProperties.buildPluginProperties(ImmutableMap.<String, String>of(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, "1234")),
                                                                                                            context);

        assertEquals(authorizationInfoPlugin.getGatewayError(), "CVC Declined");
        final List<PaymentTransactionInfoPlugin> fromDBList = adyenPaymentPluginApi.getPaymentInfo(account.getId(), payment.getId(), ImmutableList.<PluginProperty>of(), context);
        assertFalse(fromDBList.isEmpty());
        assertEquals(fromDBList.get(0).getGatewayError(), "CVC Declined");
    }

    @Test(groups = "integration")
    public void testAuthorizeRecurringDetailRecurring() throws Exception {
        final Iterable<PluginProperty> propertiesWithCCForRecurring = PluginProperties.buildPluginProperties(propertiesForRecurring);
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCForRecurring, context);

        final Payment payment = doAuthorize(BigDecimal.TEN, PluginProperties.merge(ImmutableMap.<String, String>of(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE,
                                                                                                                   AdyenPaymentPluginApi.PROPERTY_RECURRING_TYPE, "RECURRING"),
                                                                                   propertiesWithCCInfo));
        doCapture(payment, BigDecimal.TEN);
        doRefund(payment, BigDecimal.TEN);

        // Sleep a few seconds to give Adyen's Test System time to process and create the RecurringDetails
        Thread.sleep(SLEEP_IN_MILLIS_FOR_RECURRING_DETAIL);

        final List<RecurringDetail> recurringDetailList = adyenRecurringClient.getRecurringDetailList(propertiesForRecurring.get(AdyenPaymentPluginApi.PROPERTY_CUSTOMER_ID),
                                                                                                      adyenConfigProperties.getMerchantAccount(DEFAULT_COUNTRY),
                                                                                                      "RECURRING");
        if (recurringDetailList.isEmpty()) {
            fail("No recurring details for " + propertiesForRecurring.get(AdyenPaymentPluginApi.PROPERTY_CUSTOMER_ID));
        }

        final Iterable<PluginProperty> propertiesWithRecurringDetailInfo = PluginProperties.merge(ImmutableMap.<String, String>builder()
                                                                                                          .putAll(propertiesForRecurring)
                                                                                                          .put(AdyenPaymentPluginApi.PROPERTY_RECURRING_DETAIL_ID, recurringDetailList.get(0).getRecurringDetailReference())
                                                                                                          .put(AdyenPaymentPluginApi.PROPERTY_RECURRING_TYPE, "RECURRING")
                                                                                                          .build());
        final Payment payment2 = doAuthorize(BigDecimal.TEN, propertiesWithRecurringDetailInfo);
        doCapture(payment2, BigDecimal.TEN);
        doRefund(payment2, BigDecimal.TEN);
    }

    @Test(groups = "integration")
    public void testAuthorizeRecurringDetailOneClick() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, ImmutableList.<PluginProperty>of(), context);

        final Iterable<PluginProperty> propertiesWithCCForRecurring = PluginProperties.merge(ImmutableMap.<String, String>builder()
                                                                                                     .putAll(propertiesForRecurring)
                                                                                                     .put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE)
                                                                                                     .put(AdyenPaymentPluginApi.PROPERTY_RECURRING_TYPE, "ONECLICK")
                                                                                                     .build(),
                                                                                             propertiesWithCCInfo);
        final Payment payment = doAuthorize(BigDecimal.TEN, propertiesWithCCForRecurring);
        doCapture(payment, BigDecimal.TEN);
        doRefund(payment, BigDecimal.TEN);

        // Sleep a few seconds to give Adyen's Test System time to process and create the RecurringDetails
        Thread.sleep(SLEEP_IN_MILLIS_FOR_RECURRING_DETAIL);

        final List<RecurringDetail> recurringDetailList = adyenRecurringClient.getRecurringDetailList(propertiesForRecurring.get(AdyenPaymentPluginApi.PROPERTY_CUSTOMER_ID),
                                                                                                      adyenConfigProperties.getMerchantAccount(DEFAULT_COUNTRY),
                                                                                                      "ONECLICK");
        if (recurringDetailList.isEmpty()) {
            fail("No recurring details for " + propertiesForRecurring.get(AdyenPaymentPluginApi.PROPERTY_CUSTOMER_ID));
        }

        final Iterable<PluginProperty> propertiesWithRecurringDetailInfo = PluginProperties.buildPluginProperties(ImmutableMap.<String, String>builder()
                                                                                                                          .putAll(propertiesForRecurring)
                                                                                                                          .put(AdyenPaymentPluginApi.PROPERTY_RECURRING_DETAIL_ID, recurringDetailList.get(0).getRecurringDetailReference())
                                                                                                                          .put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE)
                                                                                                                          .put(AdyenPaymentPluginApi.PROPERTY_RECURRING_TYPE, "ONECLICK")
                                                                                                                          .build());
        final Payment payment2 = doAuthorize(BigDecimal.TEN, propertiesWithRecurringDetailInfo);
        doCapture(payment2, BigDecimal.TEN);
        doRefund(payment2, BigDecimal.TEN);
    }

    @Test(groups = "integration")
    public void testAuthorizeWithContinuousAuthentication() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCInfo, context);

        final Payment payment = doAuthorize(BigDecimal.TEN, PluginProperties.buildPluginProperties(ImmutableMap.<String, String>of(AdyenPaymentPluginApi.PROPERTY_CONTINUOUS_AUTHENTICATION, "true")));
        doCapture(payment, BigDecimal.TEN);
        doRefund(payment, BigDecimal.TEN);
    }

    @Test(groups = "integration")
    public void testAuthorizeAndCheckAVSResult() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithAVSInfo, context);
        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);
        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("1000"), account.getCurrency());

        final PaymentTransactionInfoPlugin authorizationInfoPlugin1 = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                             payment.getId(),
                                                                                                             authorizationTransaction.getId(),
                                                                                                             account.getPaymentMethodId(),
                                                                                                             authorizationTransaction.getAmount(),
                                                                                                             authorizationTransaction.getCurrency(),
                                                                                                             propertiesWithAVSInfo,
                                                                                                             context);
        assertNull(authorizationInfoPlugin1.getGatewayErrorCode());
        assertEquals(authorizationInfoPlugin1.getStatus(), PaymentPluginStatus.PROCESSED);

        final String avsResult = PluginProperties.findPluginPropertyValue("avsResult", authorizationInfoPlugin1.getProperties());
        final String avsResultCode = PluginProperties.findPluginPropertyValue("avsResultCode", authorizationInfoPlugin1.getProperties());

        assertEquals(avsResultCode, "Y");
        assertEquals(avsResult, "7 Both postal code and address match");
    }

    @Test(groups = "integration")
    public void testAuthorizeWithGooglePay() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithGooglePay, context);
        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);

        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("25"), account.getCurrency());

        final PaymentTransactionInfoPlugin authorizationInfoPlugin = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                            payment.getId(),
                                                                                                            authorizationTransaction.getId(),
                                                                                                            account.getPaymentMethodId(),
                                                                                                            authorizationTransaction.getAmount(),
                                                                                                            authorizationTransaction.getCurrency(),
                                                                                                            propertiesWithGooglePay,
                                                                                                            context);

        assertNull(authorizationInfoPlugin.getGatewayErrorCode());
        assertEquals(authorizationInfoPlugin.getStatus(), PaymentPluginStatus.PROCESSED);
    }

    @Test(groups = "integration")
    public void testAuthorizeWithApplePay() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithApplePay, context);
        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);

        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("60"), account.getCurrency());

        final PaymentTransactionInfoPlugin authorizationInfoPlugin = adyenPaymentPluginApi.authorizePayment(account.getId(),
                payment.getId(),
                authorizationTransaction.getId(),
                account.getPaymentMethodId(),
                authorizationTransaction.getAmount(),
                authorizationTransaction.getCurrency(),
                propertiesWithApplePay,
                context);

        assertNull(authorizationInfoPlugin.getGatewayErrorCode());
        assertEquals(authorizationInfoPlugin.getStatus(), PaymentPluginStatus.PROCESSED);
    }

    @Test(groups = "integration")
    public void testAuthorizeWithApplePayIgnoreRecurring() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithApplePayRecurring, context);
        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);

        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("60"), account.getCurrency());

        final PaymentTransactionInfoPlugin authorizationInfoPlugin = adyenPaymentPluginApi.authorizePayment(account.getId(),
                payment.getId(),
                authorizationTransaction.getId(),
                account.getPaymentMethodId(),
                authorizationTransaction.getAmount(),
                authorizationTransaction.getCurrency(),
                propertiesWithApplePay,
                context);

        assertNull(authorizationInfoPlugin.getGatewayErrorCode());
        assertEquals(authorizationInfoPlugin.getStatus(), PaymentPluginStatus.PROCESSED);
    }

    @Test(groups = "integration")
    public void testAuthorizeAndCheckBadAVSResult() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithAVSInfo, context);
        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);
        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("1000"), account.getCurrency());

        final PaymentTransactionInfoPlugin authorizationInfoPlugin1 = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                             payment.getId(),
                                                                                                             authorizationTransaction.getId(),
                                                                                                             account.getPaymentMethodId(),
                                                                                                             authorizationTransaction.getAmount(),
                                                                                                             authorizationTransaction.getCurrency(),
                                                                                                             propertiesWithBadAVSInfo,
                                                                                                             context);
        assertNull(authorizationInfoPlugin1.getGatewayErrorCode());
        assertEquals(authorizationInfoPlugin1.getStatus(), PaymentPluginStatus.PROCESSED);

        final String avsResult = PluginProperties.findPluginPropertyValue("avsResult", authorizationInfoPlugin1.getProperties());
        final String avsResultCode = PluginProperties.findPluginPropertyValue("avsResultCode", authorizationInfoPlugin1.getProperties());

        assertEquals(avsResultCode, "A");
        assertEquals(avsResult, "1 Address matches, postal code doesn't");
    }

    @Test(groups = "integration")
    public void testAuthorizeWithZipCodeOnlyAVSCheck() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithZipCodeOnlyAVSInfo, context);
        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);
        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("1000"), account.getCurrency());

        final PaymentTransactionInfoPlugin authorizationInfoPlugin1 = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                             payment.getId(),
                                                                                                             authorizationTransaction.getId(),
                                                                                                             account.getPaymentMethodId(),
                                                                                                             authorizationTransaction.getAmount(),
                                                                                                             authorizationTransaction.getCurrency(),
                                                                                                             propertiesWithZipCodeOnlyAVSInfo,
                                                                                                             context);
        assertNull(authorizationInfoPlugin1.getGatewayErrorCode());
        assertEquals(authorizationInfoPlugin1.getStatus(), PaymentPluginStatus.PROCESSED);

        final String avsResult = PluginProperties.findPluginPropertyValue("avsResult", authorizationInfoPlugin1.getProperties());
        final String avsResultCode = PluginProperties.findPluginPropertyValue("avsResultCode", authorizationInfoPlugin1.getProperties());

        assertEquals(avsResultCode, "Z");
        assertEquals(avsResult, "6 Postal code matches, address doesn't match");

    }

    @Test(groups = "integration")
    public void testAuthorizeAndComplete3DSecure() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWith3DSInfo, context);

        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);

        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("1000"), account.getCurrency());
        final PaymentTransaction captureTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.CAPTURE, new BigDecimal("1000"), account.getCurrency());

        final PaymentTransactionInfoPlugin authorizationInfoPlugin1 = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                             payment.getId(),
                                                                                                             authorizationTransaction.getId(),
                                                                                                             account.getPaymentMethodId(),
                                                                                                             authorizationTransaction.getAmount(),
                                                                                                             authorizationTransaction.getCurrency(),
                                                                                                             propertiesWith3DSInfo,
                                                                                                             context);

        assertNull(authorizationInfoPlugin1.getGatewayErrorCode());
        assertEquals(authorizationInfoPlugin1.getStatus(), PaymentPluginStatus.PENDING);

        final String expectedMerchantAccount = getExpectedMerchantAccount(payment);
        final PaymentTransactionInfoPlugin paymentInfo = Iterables.getLast(adyenPaymentPluginApi.getPaymentInfo(payment.getAccountId(), payment.getId(), null, context));
        assertEquals(PluginProperties.findPluginPropertyValue("merchantAccountCode", paymentInfo.getProperties()), expectedMerchantAccount);

        // Verify GET path
        final List<PaymentTransactionInfoPlugin> paymentTransactionInfoPluginsPreCompletion = adyenPaymentPluginApi.getPaymentInfo(account.getId(),
                                                                                                                                   authorizationInfoPlugin1.getKbPaymentId(),
                                                                                                                                   ImmutableList.<PluginProperty>of(),
                                                                                                                                   context);
        assertEquals(paymentTransactionInfoPluginsPreCompletion.size(), 1);
        assertEquals(paymentTransactionInfoPluginsPreCompletion.get(0).getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(paymentTransactionInfoPluginsPreCompletion.get(0).getStatus(), PaymentPluginStatus.PENDING);

        final URL issuerUrl = new URL(PluginProperties.findPluginPropertyValue("issuerUrl", authorizationInfoPlugin1.getProperties()));
        final String md = PluginProperties.findPluginPropertyValue("MD", authorizationInfoPlugin1.getProperties());
        final String paReq = PluginProperties.findPluginPropertyValue("PaReq", authorizationInfoPlugin1.getProperties());
        final String termUrl = PluginProperties.findPluginPropertyValue("TermUrl", authorizationInfoPlugin1.getProperties());

        final String responseHTML = given().log().all()
                                           .contentType(ContentType.URLENC)
                                           .accept(ContentType.HTML)
                                           .formParam("MD", md)
                                           .formParam("PaReq", paReq)
                                           .formParam("TermUrl", termUrl)
                                           .post(issuerUrl)
                                           .then().log().all()
                                           .statusCode(HTTP_200_OK)
                                           .extract().asString();

        final Map<String, String> formParams = extractForm(responseHTML);
        assertFalse(formParams.isEmpty(), "No FORM found in HTML response");

        final String formAction = rewriteFormURL(issuerUrl, formParams.remove("formAction"));
        formParams.put("username", "user");
        formParams.put("password", "password");

        final String redirectHTML = given().log().all()
                .contentType(ContentType.URLENC)
                .accept(ContentType.HTML)
                .formParams(formParams)
                .post(formAction)
                .then().log().all()
                .statusCode(HTTP_200_OK)
                .extract().asString();

        final Map<String, String> redirectFormParams = extractForm(redirectHTML);
        assertFalse(redirectFormParams.isEmpty(), "No FORM found in redirect HTML response");

        assertEquals(termUrl, redirectFormParams.remove("formAction"));
        // simulate url encoding that happens in the KillBill Client
        redirectFormParams.put("MD", UTF8UrlEncoder.encodeQueryElement(redirectFormParams.get("MD")));
        redirectFormParams.put("PaRes", UTF8UrlEncoder.encodeQueryElement(redirectFormParams.get("PaRes")));

        final List<PluginProperty> propertiesWithCompleteParams = PluginProperties.buildPluginProperties(redirectFormParams);

        final PaymentTransactionInfoPlugin authorizationInfoPlugin2 = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                             payment.getId(),
                                                                                                             authorizationTransaction.getId(),
                                                                                                             account.getPaymentMethodId(),
                                                                                                             authorizationTransaction.getAmount(),
                                                                                                             authorizationTransaction.getCurrency(),
                                                                                                             propertiesWithCompleteParams,
                                                                                                             context);

        verifyPaymentTransactionInfoPlugin(payment, authorizationTransaction, authorizationInfoPlugin2);
        assertEquals(authorizationInfoPlugin2.getFirstPaymentReferenceId(), authorizationInfoPlugin1.getFirstPaymentReferenceId());

        // Verify GET path
        final List<PaymentTransactionInfoPlugin> paymentTransactionInfoPluginsPostCompletion = adyenPaymentPluginApi.getPaymentInfo(account.getId(),
                                                                                                                                    authorizationInfoPlugin1.getKbPaymentId(),
                                                                                                                                    ImmutableList.<PluginProperty>of(),
                                                                                                                                    context);
        assertEquals(paymentTransactionInfoPluginsPostCompletion.size(), 1);
        assertEquals(paymentTransactionInfoPluginsPostCompletion.get(0).getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(paymentTransactionInfoPluginsPostCompletion.get(0).getStatus(), PaymentPluginStatus.PROCESSED);

        final PaymentTransactionInfoPlugin captureInfoPlugin = adyenPaymentPluginApi.capturePayment(account.getId(),
                                                                                                    payment.getId(),
                                                                                                    captureTransaction.getId(),
                                                                                                    account.getPaymentMethodId(),
                                                                                                    captureTransaction.getAmount(),
                                                                                                    captureTransaction.getCurrency(),
                                                                                                    authorizationInfoPlugin2.getProperties(),
                                                                                                    context);

        verifyPaymentTransactionInfoPlugin(payment, captureTransaction, captureInfoPlugin);

        // Verify GET path
        final List<PaymentTransactionInfoPlugin> paymentTransactionInfoPluginsPostCapture = adyenPaymentPluginApi.getPaymentInfo(account.getId(),
                                                                                                                                 authorizationInfoPlugin1.getKbPaymentId(),
                                                                                                                                 ImmutableList.<PluginProperty>of(),
                                                                                                                                 context);
        assertEquals(paymentTransactionInfoPluginsPostCapture.size(), 2);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(0).getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(0).getStatus(), PaymentPluginStatus.PROCESSED);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(1).getTransactionType(), TransactionType.CAPTURE);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(1).getStatus(), PaymentPluginStatus.PENDING);
    }

    @Test(groups = "integration")
    public void testAuthorize3DS2IdentifyAndChallengeShopper() throws Exception {
        // Setup
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesFor3DS2IdentifyShopper, context);

        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);
        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("12100"), account.getCurrency());
        final PaymentTransaction captureTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.CAPTURE, new BigDecimal("12100"), account.getCurrency());
        final String expectedMerchantAccount = getExpectedMerchantAccount(payment);

        // Initial authorize call
        final PaymentTransactionInfoPlugin authorizeResult = adyenPaymentPluginApi.authorizePayment(
                account.getId(),
                payment.getId(),
                authorizationTransaction.getId(),
                account.getPaymentMethodId(),
                authorizationTransaction.getAmount(),
                authorizationTransaction.getCurrency(),
                propertiesFor3DS2IdentifyShopper,
                context);
        final UUID kbPaymentId = authorizeResult.getKbPaymentId();

        final String threeDSServerTransID = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS_SERVER_TRANS_ID, authorizeResult.getProperties());
        final String threeDS2Token = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS2_TOKEN, authorizeResult.getProperties());
        final URL threeDSMethodURL = new URL(PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS_METHOD_URL, authorizeResult.getProperties()));

        assertNull(authorizeResult.getGatewayErrorCode());
        assertNotNull(threeDSServerTransID);
        assertNotNull(threeDS2Token);
        assertNotNull(threeDSMethodURL);

        final PaymentTransactionInfoPlugin paymentInfo = Iterables.getLast(adyenPaymentPluginApi.getPaymentInfo(payment.getAccountId(), payment.getId(), null, context));
        assertEquals(PluginProperties.findPluginPropertyValue("merchantAccountCode", paymentInfo.getProperties()), expectedMerchantAccount);

        // Verify GET path, this also allows us to check the result code that the KB caller sees
        final List<PaymentTransactionInfoPlugin> initialPaymentTransactions = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(initialPaymentTransactions.size(), 1);

        final AdyenPaymentTransactionInfoPlugin adyenInfoObj = (AdyenPaymentTransactionInfoPlugin)initialPaymentTransactions.get(0);
        final String pspReference = adyenInfoObj.getAdyenResponseRecord().get().getPspReference();

        assertEquals(adyenInfoObj.getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(adyenInfoObj.getAdyenResponseRecord().get().getResultCode(), "IdentifyShopper");

        // Mimic POST from the hidden iframe that generates the device fingerprint, to the threeDSMethodURL
        final String notificationURL = PluginProperties.findPluginPropertyValue(PROPERTY_NOTIFICATION_URL, propertiesFor3DS2IdentifyShopper);
        final Map<String, String> threeDSMethodData = ImmutableMap.of(
                PROPERTY_THREEDS_SERVER_TRANS_ID, threeDSServerTransID,
                PROPERTY_METHOD_NOTIFICATION_URL, notificationURL);

        final String identifyResponse = given().log().all()
                .contentType(ContentType.URLENC)
                .accept(ContentType.HTML)
                .formParam("threeDSMethodData", toJsonAndEncode(threeDSMethodData))
                .post(threeDSMethodURL)
                .then().log().all()
                .statusCode(HTTP_200_OK)
                .extract().asString();

        final Map<String, String> identifyResponseForm = extractForm(identifyResponse);
        assertFalse(identifyResponseForm.isEmpty(), "No FORM found in identify shopper response");

        // Simulate Notification callback
        final String returnedThreeDSMethodData = identifyResponseForm.get("threeDSMethodData");
        assertNotNull(returnedThreeDSMethodData , "No threeDSMethodData value found in identify response");

        final Map<String, Object> decodedThreeDSMethodData = new ObjectMapper().readValue(DatatypeConverter.parseBase64Binary(returnedThreeDSMethodData), new TypeReference<Map<String, Object>>(){});
        assertEquals(decodedThreeDSMethodData.get(PROPERTY_THREEDS_SERVER_TRANS_ID), threeDSServerTransID);

        // Send next authorize request with completion indicator set to "Y"

        // We don't expect the caller to send us the threeDS2Token as we can get it internally
        final List<PluginProperty> propertiesWithCompInd = PluginProperties.buildPluginProperties(ImmutableMap.of(PROPERTY_THREEDS_COMP_IND, "Y"));
        final PaymentTransactionInfoPlugin identificationCompleteResponse = adyenPaymentPluginApi.authorizePayment(account.getId(),
                payment.getId(),
                authorizationTransaction.getId(),
                account.getPaymentMethodId(),
                authorizationTransaction.getAmount(),
                authorizationTransaction.getCurrency(),
                propertiesWithCompInd,
                context);

        // Verify GET path, this also allows us to check the result code that the KB caller sees
        final List<PaymentTransactionInfoPlugin> initialPaymentTransactionsAfterIdentification = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(initialPaymentTransactionsAfterIdentification.size(), 1);
        assertTrue(initialPaymentTransactionsAfterIdentification.get(0) instanceof AdyenPaymentTransactionInfoPlugin);

        final AdyenPaymentTransactionInfoPlugin adyenInfoObj2 = (AdyenPaymentTransactionInfoPlugin)initialPaymentTransactionsAfterIdentification.get(0);
        assertEquals(adyenInfoObj.getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(adyenInfoObj2.getAdyenResponseRecord().get().getPspReference(), pspReference);
        assertEquals(adyenInfoObj2.getAdyenResponseRecord().get().getResultCode(), "ChallengeShopper");

        final String messageVersion = PluginProperties.findPluginPropertyValue(PROPERTY_MESSAGE_VERSION, identificationCompleteResponse.getProperties());
        final String transId = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS_SERVER_TRANS_ID, identificationCompleteResponse.getProperties());
        final String acsTransId = PluginProperties.findPluginPropertyValue(PROPERTY_ACS_TRANS_ID, identificationCompleteResponse.getProperties());
        final URL acsUrl = new URL(PluginProperties.findPluginPropertyValue(PROPERTY_ACS_URL, identificationCompleteResponse.getProperties()));
        final String threeDS2TokenInResponse = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS2_TOKEN, identificationCompleteResponse.getProperties());

        assertEquals(threeDS2TokenInResponse, threeDS2Token);
        assertNotNull(messageVersion);
        assertNotNull(transId);
        assertNotNull(acsTransId);

        // Mimic POST from the challenge iframe to the acsURL

        // All these fields are available in the response from the previous authoriz call
        Map<String, String> creqData = ImmutableMap.of(
                PROPERTY_THREEDS_SERVER_TRANS_ID, transId,
                PROPERTY_ACS_TRANS_ID, acsTransId,
                PROPERTY_MESSAGE_VERSION, messageVersion,
                "challengeWindowSize", "05",
                "messageType","CReq");

        // This is the original POST when loading the iframe - it returns the actual challenge page for the user
        final String captureResponse = given().log().all()
                .contentType(ContentType.URLENC)
                .accept(ContentType.HTML)
                .formParam("creq", toJsonAndEncode(creqData))
                .post(acsUrl)
                .then().log().all()
                .statusCode(HTTP_200_OK)
                .extract().asString();

        final Map<String, String> challengeForm = extractForm(captureResponse);
        assertFalse(challengeForm.isEmpty(), "No FORM found in challenge page");

        final String postChallengeUrl = rewriteFormURL(acsUrl, challengeForm.remove("formAction"));
        challengeForm.put("answer", "password");

        // This is simulating the challenge response POST from the from to the bank
        final String challengeResponse = given().log().all()
                .contentType(ContentType.URLENC)
                .accept(ContentType.HTML)
                .formParams(challengeForm)
                .post(postChallengeUrl)
                .then().log().all()
                .statusCode(HTTP_200_OK)
                .extract().asString();

        final Map<String, String> challengeResponseForm = extractForm(challengeResponse);
        assertFalse(challengeResponseForm.isEmpty(), "No FORM found in challenge response");

        // Simulate Notification callback
        final String cres = challengeResponseForm.get("cres");
        assertNotNull(cres, "No cres value found in challenge response");

        final Map<String, Object> decodedCres = new ObjectMapper().readValue(DatatypeConverter.parseBase64Binary(cres), new TypeReference<Map<String, Object>>(){});
        final String transStatus = decodedCres.get(PROPERTY_TRANS_STATUS).toString();

        assertEquals(decodedCres.get("messageType"), "CRes");
        assertEquals(transStatus , "Y");

        // Send next authorize request with transStatus

        // We don't expect the caller to send us the threeDS2Token as we can get it internally
        final List<PluginProperty> challengeResultProps = PluginProperties.buildPluginProperties(ImmutableMap.of(PROPERTY_TRANS_STATUS, transStatus));
        final PaymentTransactionInfoPlugin finalAuthorizationResponse = adyenPaymentPluginApi.authorizePayment(account.getId(),
                payment.getId(),
                authorizationTransaction.getId(),
                account.getPaymentMethodId(),
                authorizationTransaction.getAmount(),
                authorizationTransaction.getCurrency(),
                challengeResultProps,
                context);

        verifyPaymentTransactionInfoPlugin(payment, authorizationTransaction, finalAuthorizationResponse);
        assertEquals(finalAuthorizationResponse.getFirstPaymentReferenceId(), authorizeResult.getFirstPaymentReferenceId());

        // Verify GET path after authorize finished
        final List<PaymentTransactionInfoPlugin> paymentTransactionsAfterAuthorize = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(paymentTransactionsAfterAuthorize.size(), 1);
        assertEquals(paymentTransactionsAfterAuthorize.get(0).getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(paymentTransactionsAfterAuthorize.get(0).getStatus(), PaymentPluginStatus.PROCESSED);

        final PaymentTransactionInfoPlugin captureInfoPlugin = adyenPaymentPluginApi.capturePayment(
                account.getId(),
                payment.getId(),
                captureTransaction.getId(),
                account.getPaymentMethodId(),
                captureTransaction.getAmount(),
                captureTransaction.getCurrency(),
                finalAuthorizationResponse.getProperties(),
                context);

        verifyPaymentTransactionInfoPlugin(payment, captureTransaction, captureInfoPlugin);

        // Verify GET path
        final List<PaymentTransactionInfoPlugin> paymentTransactionInfoPluginsPostCapture = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(paymentTransactionInfoPluginsPostCapture.size(), 2);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(0).getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(0).getStatus(), PaymentPluginStatus.PROCESSED);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(1).getTransactionType(), TransactionType.CAPTURE);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(1).getStatus(), PaymentPluginStatus.PENDING);
    }

    @Test(groups = "integration")
    public void testAuthorize3DS2IdentifyAndChallengeShopperContAuth() throws Exception {
        // Setup
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesFor3DS2IdentifyShopperNoCVC, context);

        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);
        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("12100"), account.getCurrency());
        final PaymentTransaction captureTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.CAPTURE, new BigDecimal("12100"), account.getCurrency());
        final String expectedMerchantAccount = getExpectedMerchantAccount(payment);

        // Initial authorize call
        final PaymentTransactionInfoPlugin authorizeResult = adyenPaymentPluginApi.authorizePayment(
                account.getId(),
                payment.getId(),
                authorizationTransaction.getId(),
                account.getPaymentMethodId(),
                authorizationTransaction.getAmount(),
                authorizationTransaction.getCurrency(),
                propertiesFor3DS2IdentifyShopperNoCVC,
                context);
        final UUID kbPaymentId = authorizeResult.getKbPaymentId();

        final String threeDSServerTransID = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS_SERVER_TRANS_ID, authorizeResult.getProperties());
        final String threeDS2Token = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS2_TOKEN, authorizeResult.getProperties());
        final URL threeDSMethodURL = new URL(PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS_METHOD_URL, authorizeResult.getProperties()));

        assertNull(authorizeResult.getGatewayErrorCode());
        assertNotNull(threeDSServerTransID);
        assertNotNull(threeDS2Token);
        assertNotNull(threeDSMethodURL);

        final PaymentTransactionInfoPlugin paymentInfo = Iterables.getLast(adyenPaymentPluginApi.getPaymentInfo(payment.getAccountId(), payment.getId(), null, context));
        assertEquals(PluginProperties.findPluginPropertyValue("merchantAccountCode", paymentInfo.getProperties()), expectedMerchantAccount);

        // Verify GET path, this also allows us to check the result code that the KB caller sees
        final List<PaymentTransactionInfoPlugin> initialPaymentTransactions = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(initialPaymentTransactions.size(), 1);

        final AdyenPaymentTransactionInfoPlugin adyenInfoObj = (AdyenPaymentTransactionInfoPlugin)initialPaymentTransactions.get(0);
        final String pspReference = adyenInfoObj.getAdyenResponseRecord().get().getPspReference();

        assertEquals(adyenInfoObj.getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(adyenInfoObj.getAdyenResponseRecord().get().getResultCode(), "IdentifyShopper");

        // Mimic POST from the hidden iframe that generates the device fingerprint, to the threeDSMethodURL
        final String notificationURL = PluginProperties.findPluginPropertyValue(PROPERTY_NOTIFICATION_URL, propertiesFor3DS2IdentifyShopper);
        final Map<String, String> threeDSMethodData = ImmutableMap.of(
                PROPERTY_THREEDS_SERVER_TRANS_ID, threeDSServerTransID,
                PROPERTY_METHOD_NOTIFICATION_URL, notificationURL);

        final String identifyResponse = given().log().all()
                                               .contentType(ContentType.URLENC)
                                               .accept(ContentType.HTML)
                                               .formParam("threeDSMethodData", toJsonAndEncode(threeDSMethodData))
                                               .post(threeDSMethodURL)
                                               .then().log().all()
                                               .statusCode(HTTP_200_OK)
                                               .extract().asString();

        final Map<String, String> identifyResponseForm = extractForm(identifyResponse);
        assertFalse(identifyResponseForm.isEmpty(), "No FORM found in identify shopper response");

        // Simulate Notification callback
        final String returnedThreeDSMethodData = identifyResponseForm.get("threeDSMethodData");
        assertNotNull(returnedThreeDSMethodData , "No threeDSMethodData value found in identify response");

        final Map<String, Object> decodedThreeDSMethodData = new ObjectMapper().readValue(DatatypeConverter.parseBase64Binary(returnedThreeDSMethodData), new TypeReference<Map<String, Object>>(){});
        assertEquals(decodedThreeDSMethodData.get(PROPERTY_THREEDS_SERVER_TRANS_ID), threeDSServerTransID);

        // Send next authorize request with completion indicator set to "Y"

        // We don't expect the caller to send us the threeDS2Token as we can get it internally
        final List<PluginProperty> propertiesWithCompInd = PluginProperties.buildPluginProperties(ImmutableMap.of(PROPERTY_THREEDS_COMP_IND, "Y"));
        final PaymentTransactionInfoPlugin identificationCompleteResponse = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                                   payment.getId(),
                                                                                                                   authorizationTransaction.getId(),
                                                                                                                   account.getPaymentMethodId(),
                                                                                                                   authorizationTransaction.getAmount(),
                                                                                                                   authorizationTransaction.getCurrency(),
                                                                                                                   propertiesWithCompInd,
                                                                                                                   context);

        // Verify GET path, this also allows us to check the result code that the KB caller sees
        final List<PaymentTransactionInfoPlugin> initialPaymentTransactionsAfterIdentification = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(initialPaymentTransactionsAfterIdentification.size(), 1);
        assertTrue(initialPaymentTransactionsAfterIdentification.get(0) instanceof AdyenPaymentTransactionInfoPlugin);

        final AdyenPaymentTransactionInfoPlugin adyenInfoObj2 = (AdyenPaymentTransactionInfoPlugin)initialPaymentTransactionsAfterIdentification.get(0);
        assertEquals(adyenInfoObj.getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(adyenInfoObj2.getAdyenResponseRecord().get().getPspReference(), pspReference);
        assertEquals(adyenInfoObj2.getAdyenResponseRecord().get().getResultCode(), "ChallengeShopper");

        final String messageVersion = PluginProperties.findPluginPropertyValue(PROPERTY_MESSAGE_VERSION, identificationCompleteResponse.getProperties());
        final String transId = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS_SERVER_TRANS_ID, identificationCompleteResponse.getProperties());
        final String acsTransId = PluginProperties.findPluginPropertyValue(PROPERTY_ACS_TRANS_ID, identificationCompleteResponse.getProperties());
        final URL acsUrl = new URL(PluginProperties.findPluginPropertyValue(PROPERTY_ACS_URL, identificationCompleteResponse.getProperties()));
        final String threeDS2TokenInResponse = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS2_TOKEN, identificationCompleteResponse.getProperties());

        assertEquals(threeDS2TokenInResponse, threeDS2Token);
        assertNotNull(messageVersion);
        assertNotNull(transId);
        assertNotNull(acsTransId);

        // Mimic POST from the challenge iframe to the acsURL

        // All these fields are available in the response from the previous authoriz call
        Map<String, String> creqData = ImmutableMap.of(
                PROPERTY_THREEDS_SERVER_TRANS_ID, transId,
                PROPERTY_ACS_TRANS_ID, acsTransId,
                PROPERTY_MESSAGE_VERSION, messageVersion,
                "challengeWindowSize", "05",
                "messageType","CReq");

        // This is the original POST when loading the iframe - it returns the actual challenge page for the user
        final String captureResponse = given().log().all()
                                              .contentType(ContentType.URLENC)
                                              .accept(ContentType.HTML)
                                              .formParam("creq", toJsonAndEncode(creqData))
                                              .post(acsUrl)
                                              .then().log().all()
                                              .statusCode(HTTP_200_OK)
                                              .extract().asString();

        final Map<String, String> challengeForm = extractForm(captureResponse);
        assertFalse(challengeForm.isEmpty(), "No FORM found in challenge page");

        final String postChallengeUrl = rewriteFormURL(acsUrl, challengeForm.remove("formAction"));
        challengeForm.put("answer", "password");

        // This is simulating the challenge response POST from the from to the bank
        final String challengeResponse = given().log().all()
                                                .contentType(ContentType.URLENC)
                                                .accept(ContentType.HTML)
                                                .formParams(challengeForm)
                                                .post(postChallengeUrl)
                                                .then().log().all()
                                                .statusCode(HTTP_200_OK)
                                                .extract().asString();

        final Map<String, String> challengeResponseForm = extractForm(challengeResponse);
        assertFalse(challengeResponseForm.isEmpty(), "No FORM found in challenge response");

        // Simulate Notification callback
        final String cres = challengeResponseForm.get("cres");
        assertNotNull(cres, "No cres value found in challenge response");

        final Map<String, Object> decodedCres = new ObjectMapper().readValue(DatatypeConverter.parseBase64Binary(cres), new TypeReference<Map<String, Object>>(){});
        final String transStatus = decodedCres.get(PROPERTY_TRANS_STATUS).toString();

        assertEquals(decodedCres.get("messageType"), "CRes");
        assertEquals(transStatus , "Y");

        // Send next authorize request with transStatus

        // We don't expect the caller to send us the threeDS2Token as we can get it internally
        final List<PluginProperty> challengeResultProps = PluginProperties.buildPluginProperties(ImmutableMap.of(PROPERTY_TRANS_STATUS, transStatus));
        final PaymentTransactionInfoPlugin finalAuthorizationResponse = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                               payment.getId(),
                                                                                                               authorizationTransaction.getId(),
                                                                                                               account.getPaymentMethodId(),
                                                                                                               authorizationTransaction.getAmount(),
                                                                                                               authorizationTransaction.getCurrency(),
                                                                                                               challengeResultProps,
                                                                                                               context);

        verifyPaymentTransactionInfoPlugin(payment, authorizationTransaction, finalAuthorizationResponse);
        assertEquals(finalAuthorizationResponse.getFirstPaymentReferenceId(), authorizeResult.getFirstPaymentReferenceId());

        // Verify GET path after authorize finished
        final List<PaymentTransactionInfoPlugin> paymentTransactionsAfterAuthorize = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(paymentTransactionsAfterAuthorize.size(), 1);
        assertEquals(paymentTransactionsAfterAuthorize.get(0).getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(paymentTransactionsAfterAuthorize.get(0).getStatus(), PaymentPluginStatus.PROCESSED);

        final PaymentTransactionInfoPlugin captureInfoPlugin = adyenPaymentPluginApi.capturePayment(
                account.getId(),
                payment.getId(),
                captureTransaction.getId(),
                account.getPaymentMethodId(),
                captureTransaction.getAmount(),
                captureTransaction.getCurrency(),
                finalAuthorizationResponse.getProperties(),
                context);

        verifyPaymentTransactionInfoPlugin(payment, captureTransaction, captureInfoPlugin);

        // Verify GET path
        final List<PaymentTransactionInfoPlugin> paymentTransactionInfoPluginsPostCapture = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(paymentTransactionInfoPluginsPostCapture.size(), 2);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(0).getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(0).getStatus(), PaymentPluginStatus.PROCESSED);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(1).getTransactionType(), TransactionType.CAPTURE);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(1).getStatus(), PaymentPluginStatus.PENDING);
    }

    @Test(groups = "integration")
    public void testAuthorize3DS2ChallengeWithoutIdentify() throws Exception {
        // Setup
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesFor3DS2ChallengeShopper, context);

        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);
        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("12000"), account.getCurrency());
        final PaymentTransaction captureTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.CAPTURE, new BigDecimal("12000"), account.getCurrency());
        final String expectedMerchantAccount = getExpectedMerchantAccount(payment);

        // Initial authorize call
        final PaymentTransactionInfoPlugin authorizeResult = adyenPaymentPluginApi.authorizePayment(
                account.getId(),
                payment.getId(),
                authorizationTransaction.getId(),
                account.getPaymentMethodId(),
                authorizationTransaction.getAmount(),
                authorizationTransaction.getCurrency(),
                propertiesFor3DS2ChallengeShopper,
                context);
        final UUID kbPaymentId = authorizeResult.getKbPaymentId();

        final String threeDSServerTransID = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS_SERVER_TRANS_ID, authorizeResult.getProperties());
        final String threeDS2Token = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS2_TOKEN, authorizeResult.getProperties());
        final String acsChallengeMandated = PluginProperties.findPluginPropertyValue(PROPERTY_ACS_CHALLENGE_MANDATED, authorizeResult.getProperties());

        assertNull(authorizeResult.getGatewayErrorCode());
        assertNotNull(threeDSServerTransID);
        assertNotNull(threeDS2Token);
        assertEquals(acsChallengeMandated, "Y");

        final PaymentTransactionInfoPlugin paymentInfo = Iterables.getLast(adyenPaymentPluginApi.getPaymentInfo(payment.getAccountId(), payment.getId(), null, context));
        assertEquals(PluginProperties.findPluginPropertyValue("merchantAccountCode", paymentInfo.getProperties()), expectedMerchantAccount);

        // Verify GET path, this also allows us to check the result code that the KB caller sees
        final List<PaymentTransactionInfoPlugin> initialPaymentTransactions = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(initialPaymentTransactions.size(), 1);

        final AdyenPaymentTransactionInfoPlugin adyenInfoObj = (AdyenPaymentTransactionInfoPlugin)initialPaymentTransactions.get(0);
        final String pspReference = adyenInfoObj.getAdyenResponseRecord().get().getPspReference();

        assertEquals(adyenInfoObj.getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(adyenInfoObj.getAdyenResponseRecord().get().getResultCode(), "ChallengeShopper");

        final String messageVersion = PluginProperties.findPluginPropertyValue(PROPERTY_MESSAGE_VERSION, authorizeResult.getProperties());
        final String transId = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS_SERVER_TRANS_ID, authorizeResult.getProperties());
        final String acsTransId = PluginProperties.findPluginPropertyValue(PROPERTY_ACS_TRANS_ID, authorizeResult.getProperties());
        final URL acsUrl = new URL(PluginProperties.findPluginPropertyValue(PROPERTY_ACS_URL, authorizeResult.getProperties()));
        final String threeDS2TokenInResponse = PluginProperties.findPluginPropertyValue(PROPERTY_THREEDS2_TOKEN, authorizeResult.getProperties());

        assertEquals(threeDS2TokenInResponse, threeDS2Token);
        assertNotNull(messageVersion);
        assertNotNull(transId);
        assertNotNull(acsTransId);

        // Mimic POST from the challenge iframe to the acsURL

        // All these fields are available in the response from the previous authoriz call
        Map<String, String> creqData = ImmutableMap.of(
                PROPERTY_THREEDS_SERVER_TRANS_ID, transId,
                PROPERTY_ACS_TRANS_ID, acsTransId,
                PROPERTY_MESSAGE_VERSION, messageVersion,
                "challengeWindowSize", "05",
                "messageType","CReq");

        // This is the original POST when loading the iframe - it returns the actual challenge page for the user
        final String captureResponse = given().log().all()
                .contentType(ContentType.URLENC)
                .accept(ContentType.HTML)
                .formParam("creq", toJsonAndEncode(creqData))
                .post(acsUrl)
                .then().log().all()
                .statusCode(HTTP_200_OK)
                .extract().asString();

        final Map<String, String> challengeForm = extractForm(captureResponse);
        assertFalse(challengeForm.isEmpty(), "No FORM found in challenge page");

        final String postChallengeUrl = rewriteFormURL(acsUrl, challengeForm.remove("formAction"));
        challengeForm.put("answer", "password");

        // This is simulating the challenge response POST from the from to the bank
        final String challengeResponse = given().log().all()
                .contentType(ContentType.URLENC)
                .accept(ContentType.HTML)
                .formParams(challengeForm)
                .post(postChallengeUrl)
                .then().log().all()
                .statusCode(HTTP_200_OK)
                .extract().asString();

        final Map<String, String> challengeResponseForm = extractForm(challengeResponse);
        assertFalse(challengeResponseForm.isEmpty(), "No FORM found in challenge response");

        // Simulate Notification callback
        final String cres = challengeResponseForm.get("cres");
        assertNotNull(cres, "No cres value found in challenge response");

        final Map<String, Object> decodedCres = new ObjectMapper().readValue(DatatypeConverter.parseBase64Binary(cres), new TypeReference<Map<String, Object>>(){});
        final String transStatus = decodedCres.get(PROPERTY_TRANS_STATUS).toString();

        assertEquals(decodedCres.get("messageType"), "CRes");
        assertEquals(transStatus , "Y");

        // Send next authorize request with transStatus

        // We don't expect the caller to send us the threeDS2Token as we can get it internally
        final List<PluginProperty> challengeResultProps = PluginProperties.buildPluginProperties(ImmutableMap.of(PROPERTY_TRANS_STATUS, transStatus));
        final PaymentTransactionInfoPlugin finalAuthorizationResponse = adyenPaymentPluginApi.authorizePayment(account.getId(),
                payment.getId(),
                authorizationTransaction.getId(),
                account.getPaymentMethodId(),
                authorizationTransaction.getAmount(),
                authorizationTransaction.getCurrency(),
                challengeResultProps,
                context);

        verifyPaymentTransactionInfoPlugin(payment, authorizationTransaction, finalAuthorizationResponse);
        assertEquals(finalAuthorizationResponse.getFirstPaymentReferenceId(), authorizeResult.getFirstPaymentReferenceId());

        // Verify GET path after authorize finished
        final List<PaymentTransactionInfoPlugin> paymentTransactionsAfterAuthorize = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(paymentTransactionsAfterAuthorize.size(), 1);
        assertEquals(paymentTransactionsAfterAuthorize.get(0).getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(paymentTransactionsAfterAuthorize.get(0).getStatus(), PaymentPluginStatus.PROCESSED);

        final PaymentTransactionInfoPlugin captureInfoPlugin = adyenPaymentPluginApi.capturePayment(
                account.getId(),
                payment.getId(),
                captureTransaction.getId(),
                account.getPaymentMethodId(),
                captureTransaction.getAmount(),
                captureTransaction.getCurrency(),
                finalAuthorizationResponse.getProperties(),
                context);

        verifyPaymentTransactionInfoPlugin(payment, captureTransaction, captureInfoPlugin);

        // Verify GET path
        final List<PaymentTransactionInfoPlugin> paymentTransactionInfoPluginsPostCapture = adyenPaymentPluginApi.getPaymentInfo(
                account.getId(),
                kbPaymentId,
                ImmutableList.<PluginProperty>of(),
                context);
        assertEquals(paymentTransactionInfoPluginsPostCapture.size(), 2);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(0).getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(0).getStatus(), PaymentPluginStatus.PROCESSED);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(1).getTransactionType(), TransactionType.CAPTURE);
        assertEquals(paymentTransactionInfoPluginsPostCapture.get(1).getStatus(), PaymentPluginStatus.PENDING);
    }

    @Test(groups = "integration")
    public void testAuthorizeAndExpire3DSecure() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWith3DSInfo, context);

        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);

        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("1000"), account.getCurrency());
        final PaymentTransactionInfoPlugin authorizationInfoPlugin = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                             payment.getId(),
                                                                                                             authorizationTransaction.getId(),
                                                                                                             account.getPaymentMethodId(),
                                                                                                             authorizationTransaction.getAmount(),
                                                                                                             authorizationTransaction.getCurrency(),
                                                                                                             propertiesWith3DSInfo,
                                                                                                             context);
        assertEquals(authorizationInfoPlugin.getStatus(), PaymentPluginStatus.PENDING);

        final Period expirationPeriod = adyenConfigProperties.getPending3DsPaymentExpirationPeriod().plusMinutes(1);
        clock.setDeltaFromReality(expirationPeriod.toStandardDuration().getMillis());

        final List<PaymentTransactionInfoPlugin> expiredPaymentTransactions = adyenPaymentPluginApi.getPaymentInfo(account.getId(),
                                                                                                                   authorizationInfoPlugin.getKbPaymentId(),
                                                                                                                   ImmutableList.<PluginProperty>of(),
                                                                                                                   context);
        assertEquals(expiredPaymentTransactions.size(), 1);
        final PaymentTransactionInfoPlugin canceledTransaction = expiredPaymentTransactions.get(0);
        assertEquals(canceledTransaction.getTransactionType(), TransactionType.AUTHORIZE);
        assertEquals(canceledTransaction.getStatus(), PaymentPluginStatus.CANCELED);
    }

    @Test(groups = "integration")
    public void testAuthorizeAndCaptureWithSplitSettlements() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCInfo, context);

        final ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>();
        builder.put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE);
        builder.put(String.format("%s.1.amount", AdyenPaymentPluginApi.SPLIT_SETTLEMENT_DATA_ITEM), "3.51");
        builder.put(String.format("%s.1.group", AdyenPaymentPluginApi.SPLIT_SETTLEMENT_DATA_ITEM), "MENSWEAR");
        builder.put(String.format("%s.1.reference", AdyenPaymentPluginApi.SPLIT_SETTLEMENT_DATA_ITEM), UUID.randomUUID().toString());
        builder.put(String.format("%s.1.type", AdyenPaymentPluginApi.SPLIT_SETTLEMENT_DATA_ITEM), "FOOTWEAR");
        builder.put(String.format("%s.2.amount", AdyenPaymentPluginApi.SPLIT_SETTLEMENT_DATA_ITEM), "6.49");
        builder.put(String.format("%s.2.group", AdyenPaymentPluginApi.SPLIT_SETTLEMENT_DATA_ITEM), "STREETWEAR");
        builder.put(String.format("%s.2.reference", AdyenPaymentPluginApi.SPLIT_SETTLEMENT_DATA_ITEM), UUID.randomUUID().toString());
        builder.put(String.format("%s.2.type", AdyenPaymentPluginApi.SPLIT_SETTLEMENT_DATA_ITEM), "HEADWEAR");
        final Map<String, String> pluginPropertiesMap = builder.build();

        final List<PluginProperty> pluginProperties = PluginProperties.buildPluginProperties(pluginPropertiesMap);
        final Payment payment = doAuthorize(BigDecimal.TEN, pluginProperties);
        doCapture(payment, BigDecimal.TEN);
    }

    @Test(groups = "integration")
    public void testAuthorizeWithAdditionalData() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesWithCCInfo, context);

        final ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<String, String>();
        builder.put(AdyenPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE, CC_VERIFICATION_VALUE);
        builder.put(String.format("%s.1.key", AdyenPaymentPluginApi.ADDITIONAL_DATA_ITEM), "RequestedTestAcquirerResponseCode");
        builder.put(String.format("%s.1.value", AdyenPaymentPluginApi.ADDITIONAL_DATA_ITEM), "6");
        final Map<String, String> pluginPropertiesMap = builder.build();
        final List<PluginProperty> pluginProperties = PluginProperties.buildPluginProperties(pluginPropertiesMap);

        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);
        final PaymentTransaction authorizationTransaction = TestUtils.buildPaymentTransaction(payment, TransactionType.AUTHORIZE, new BigDecimal("1000"), account.getCurrency());
        final PaymentTransactionInfoPlugin authorizationInfoPlugin = adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                                            payment.getId(),
                                                                                                            authorizationTransaction.getId(),
                                                                                                            account.getPaymentMethodId(),
                                                                                                            authorizationTransaction.getAmount(),
                                                                                                            authorizationTransaction.getCurrency(),
                                                                                                            pluginProperties,
                                                                                                            context);

        assertEquals(authorizationInfoPlugin.getStatus(), PaymentPluginStatus.ERROR);
        assertEquals(authorizationInfoPlugin.getGatewayError(), "Expired Card");
        final List<PaymentTransactionInfoPlugin> fromDBList = adyenPaymentPluginApi.getPaymentInfo(account.getId(), payment.getId(), ImmutableList.<PluginProperty>of(), context);
        assertFalse(fromDBList.isEmpty());
        assertEquals(fromDBList.get(0).getGatewayError(), "Expired Card");
    }

    @Test(groups = "integration")
    public void testCustomerSupportMotoTransactionWithAuthCaptureAndRefund() throws Exception {
        adyenPaymentPluginApi.addPaymentMethod(account.getId(), account.getPaymentMethodId(), adyenEmptyPaymentMethodPlugin(), true, propertiesForCustomerSupportMotoPayment, context);
        final Payment payment = doAuthorize(BigDecimal.TEN);
        List<PaymentTransaction> paymentTransactions = payment.getTransactions();

        doCapture(payment, BigDecimal.TEN);
        doRefund(payment, BigDecimal.TEN);


        PaymentTransaction authPaymentTransaction = paymentTransactions.get(0);
        Assert.assertEquals(TransactionType.AUTHORIZE, authPaymentTransaction.getTransactionType());
        Assert.assertEquals(TransactionStatus.SUCCESS, authPaymentTransaction.getTransactionStatus());

        PaymentTransaction capturePaymentTransaction = paymentTransactions.get(1);
        Assert.assertEquals(TransactionType.CAPTURE, capturePaymentTransaction.getTransactionType());
        Assert.assertEquals(TransactionStatus.SUCCESS, capturePaymentTransaction.getTransactionStatus());

        PaymentTransaction refundPaymentTransaction = paymentTransactions.get(2);
        Assert.assertEquals(TransactionType.REFUND, refundPaymentTransaction.getTransactionType());
        Assert.assertEquals(TransactionStatus.SUCCESS, refundPaymentTransaction.getTransactionStatus());
    }

    private String toJsonAndEncode(Map<String, String> data) throws JsonProcessingException {
        String json = new ObjectMapper().writeValueAsString(data);
        return Base64.getEncoder().encodeToString(json.getBytes(Charsets.UTF_8));
    }

    private Map<String, String> extractForm(final String html) {
        final Map<String, String> fields = new HashMap<String, String>();
        final Document doc = Jsoup.parse(html);
        final Elements forms = doc.getElementsByTag("form");
        for (final Element form : forms) {
            if ("post".equalsIgnoreCase(form.attr("method"))) {
                fields.put("formAction", form.attr("action"));
                final Elements inputs = form.getElementsByTag("input");
                for (final Element input : inputs) {
                    final String value = input.val();
                    if (value != null && !value.isEmpty() && !"Submit".equalsIgnoreCase(value)) {
                        fields.put(input.attr("name"), value);
                    }
                }
                return fields;
            }
        }
        return Collections.emptyMap();
    }

    private String rewriteFormURL(final URL issuerUrl, final String formAction) {
        if (formAction == null || formAction.isEmpty()) {
            return issuerUrl.toString();
        } else if (formAction.startsWith("http")) {
            return formAction;
        } else {
            return issuerUrl.getProtocol() + "://" + issuerUrl.getHost() + (issuerUrl.getPort() != HTTP_PORT && issuerUrl.getPort() != HTTPS_PORT ? ":" + issuerUrl.getPort() : "") + formAction;
        }
    }

    private Payment doAuthorize(final BigDecimal amount) throws PaymentPluginApiException, PaymentApiException {
        return doAuthorize(amount, ImmutableList.<PluginProperty>of());
    }

    private Payment doAuthorize(final BigDecimal amount, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException, PaymentApiException {
        return doPluginCall(amount,
                            pluginProperties,
                            new PluginCall() {
                                @Override
                                public PaymentTransactionInfoPlugin execute(final Payment payment, final PaymentTransaction paymentTransaction, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException {
                                    return adyenPaymentPluginApi.authorizePayment(account.getId(),
                                                                                  payment.getId(),
                                                                                  paymentTransaction.getId(),
                                                                                  payment.getPaymentMethodId(),
                                                                                  paymentTransaction.getAmount(),
                                                                                  paymentTransaction.getCurrency(),
                                                                                  pluginProperties,
                                                                                  context);
                                }
                            });
    }

    private Payment doPurchase(final BigDecimal amount, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException, PaymentApiException {
        return doPluginCall(amount,
                            pluginProperties,
                            new PluginCall() {
                                @Override
                                public PaymentTransactionInfoPlugin execute(final Payment payment, final PaymentTransaction paymentTransaction, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException {
                                    return adyenPaymentPluginApi.purchasePayment(account.getId(),
                                                                                 payment.getId(),
                                                                                 paymentTransaction.getId(),
                                                                                 payment.getPaymentMethodId(),
                                                                                 paymentTransaction.getAmount(),
                                                                                 paymentTransaction.getCurrency(),
                                                                                 pluginProperties,
                                                                                 context);
                                }
                            });
    }

    private Payment doCredit(final BigDecimal amount, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException, PaymentApiException {
        return doPluginCall(amount,
                            pluginProperties,
                            new PluginCall() {
                                @Override
                                public PaymentTransactionInfoPlugin execute(final Payment payment, final PaymentTransaction paymentTransaction, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException {
                                    return adyenPaymentPluginApi.creditPayment(account.getId(),
                                                                               payment.getId(),
                                                                               paymentTransaction.getId(),
                                                                               payment.getPaymentMethodId(),
                                                                               paymentTransaction.getAmount(),
                                                                               paymentTransaction.getCurrency(),
                                                                               pluginProperties,
                                                                               context);
                                }
                            });
    }

    private Payment doCapture(final Payment payment, final BigDecimal amount) throws PaymentPluginApiException {
        return doCapture(payment, amount, ImmutableList.<PluginProperty>of());
    }

    private Payment doCapture(final Payment payment, final BigDecimal amount, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException {
        return doPluginCall(payment,
                            amount,
                            pluginProperties,
                            new PluginCall() {
                                @Override
                                public PaymentTransactionInfoPlugin execute(final Payment payment, final PaymentTransaction paymentTransaction, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException {
                                    return adyenPaymentPluginApi.capturePayment(account.getId(),
                                                                                payment.getId(),
                                                                                paymentTransaction.getId(),
                                                                                payment.getPaymentMethodId(),
                                                                                paymentTransaction.getAmount(),
                                                                                paymentTransaction.getCurrency(),
                                                                                pluginProperties,
                                                                                context);
                                }
                            });
    }

    private Payment doRefund(final Payment payment, final BigDecimal amount) throws PaymentPluginApiException {
        return doRefund(payment, amount, ImmutableList.<PluginProperty>of());
    }

    private Payment doRefund(final Payment payment, final BigDecimal amount, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException {
        return doPluginCall(payment,
                            amount,
                            pluginProperties,
                            new PluginCall() {
                                @Override
                                public PaymentTransactionInfoPlugin execute(final Payment payment, final PaymentTransaction paymentTransaction, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException {
                                    return adyenPaymentPluginApi.refundPayment(account.getId(),
                                                                               payment.getId(),
                                                                               paymentTransaction.getId(),
                                                                               payment.getPaymentMethodId(),
                                                                               paymentTransaction.getAmount(),
                                                                               paymentTransaction.getCurrency(),
                                                                               pluginProperties,
                                                                               context);
                                }
                            });
    }

    private Payment doVoid(final Payment payment) throws PaymentPluginApiException {
        return doVoid(payment, ImmutableList.<PluginProperty>of());
    }

    private Payment doVoid(final Payment payment, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException {
        return doPluginCall(payment,
                            null,
                            pluginProperties,
                            new PluginCall() {
                                @Override
                                public PaymentTransactionInfoPlugin execute(final Payment payment, final PaymentTransaction paymentTransaction, final Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException {
                                    return adyenPaymentPluginApi.voidPayment(account.getId(),
                                                                             payment.getId(),
                                                                             paymentTransaction.getId(),
                                                                             payment.getPaymentMethodId(),
                                                                             pluginProperties,
                                                                             context);
                                }
                            });
    }

    private Payment doPluginCall(final BigDecimal amount, final Iterable<PluginProperty> pluginProperties, final PluginCall pluginCall) throws PaymentPluginApiException, PaymentApiException {
        final Payment payment = TestUtils.buildPayment(account.getId(), account.getPaymentMethodId(), account.getCurrency(), killbillApi);
        return doPluginCall(payment, amount, pluginProperties, pluginCall);
    }

    private Payment doPluginCall(final Payment payment, final BigDecimal amount, final Iterable<PluginProperty> pluginProperties, final PluginCall pluginCall) throws PaymentPluginApiException {
        final PaymentTransaction paymentTransaction = TestUtils.buildPaymentTransaction(payment, null, amount, payment.getCurrency());

        final PaymentTransactionInfoPlugin paymentTransactionInfoPlugin = pluginCall.execute(payment, paymentTransaction, pluginProperties);
        TestUtils.updatePaymentTransaction(paymentTransaction, paymentTransactionInfoPlugin);

        verifyPaymentTransactionInfoPlugin(payment, paymentTransaction, paymentTransactionInfoPlugin);

        if ("true".equals(PluginProperties.findPluginPropertyValue("skip_gw", pluginProperties))) {
            return payment;
        }

        if (paymentTransactionInfoPlugin.getTransactionType() == TransactionType.AUTHORIZE ||
            paymentTransactionInfoPlugin.getTransactionType() == TransactionType.PURCHASE) {
            // Authorization are synchronous
            assertEquals(paymentTransactionInfoPlugin.getStatus(), PaymentPluginStatus.PROCESSED);
        } else {
            assertEquals(paymentTransactionInfoPlugin.getStatus(), PaymentPluginStatus.PENDING);
        }

        processNotification(TRANSACTION_TYPE_TO_EVENT_CODE.get(paymentTransaction.getTransactionType()), true, paymentTransaction.getExternalKey(), paymentTransactionInfoPlugin.getFirstPaymentReferenceId());

        assertEquals(paymentTransaction.getPaymentInfoPlugin().getStatus(), PaymentPluginStatus.PROCESSED);

        final String expectedMerchantAccount = getExpectedMerchantAccount(payment);
        final PaymentTransactionInfoPlugin refreshedTransactionInfo = Iterables.getLast(adyenPaymentPluginApi.getPaymentInfo(payment.getAccountId(), payment.getId(), null, context));
        assertEquals(PluginProperties.findPluginPropertyValue("merchantAccountCode", refreshedTransactionInfo.getProperties()), expectedMerchantAccount);

        return payment;
    }

    private String getExpectedMerchantAccount(final Payment payment) {
        final Account paymentAccount;
        try {
            paymentAccount = this.killbillApi.getAccountUserApi().getAccountById(payment.getAccountId(), context);
        } catch (AccountApiException e) {
            throw new RuntimeException(e);
        }
        return adyenConfigProperties.getMerchantAccount(paymentAccount.getCountry());
    }

    private void verifyPaymentTransactionInfoPlugin(final Payment payment, final PaymentTransaction paymentTransaction, final PaymentTransactionInfoPlugin paymentTransactionInfoPlugin) throws PaymentPluginApiException {
        verifyPaymentTransactionInfoPlugin(payment, paymentTransaction, paymentTransactionInfoPlugin, true);

        // Verify we can fetch the details
        final List<PaymentTransactionInfoPlugin> paymentTransactionInfoPlugins = adyenPaymentPluginApi.getPaymentInfo(account.getId(), paymentTransactionInfoPlugin.getKbPaymentId(), ImmutableList.<PluginProperty>of(), context);
        final PaymentTransactionInfoPlugin paymentTransactionInfoPluginFetched = Iterables.<PaymentTransactionInfoPlugin>find(Lists.<PaymentTransactionInfoPlugin>reverse(paymentTransactionInfoPlugins),
                                                                                                                              new Predicate<PaymentTransactionInfoPlugin>() {
                                                                                                                                  @Override
                                                                                                                                  public boolean apply(final PaymentTransactionInfoPlugin input) {
                                                                                                                                      return input.getKbTransactionPaymentId().equals(paymentTransaction.getId());
                                                                                                                                  }
                                                                                                                              });
        verifyPaymentTransactionInfoPlugin(payment, paymentTransaction, paymentTransactionInfoPluginFetched, true);
    }

    private void verifyPaymentTransactionInfoPlugin(final Payment payment, final PaymentTransaction paymentTransaction, final PaymentTransactionInfoPlugin paymentTransactionInfoPlugin, final boolean authorizedProcessed) {
        assertEquals(paymentTransactionInfoPlugin.getKbPaymentId(), payment.getId());
        assertEquals(paymentTransactionInfoPlugin.getKbTransactionPaymentId(), paymentTransaction.getId());
        assertEquals(paymentTransactionInfoPlugin.getTransactionType(), paymentTransaction.getTransactionType());
        if (TransactionType.VOID.equals(paymentTransaction.getTransactionType())) {
            assertNull(paymentTransactionInfoPlugin.getAmount());
            assertNull(paymentTransactionInfoPlugin.getCurrency());
        } else {
            assertEquals(paymentTransactionInfoPlugin.getAmount().compareTo(paymentTransaction.getAmount()), 0);
            assertEquals(paymentTransactionInfoPlugin.getCurrency(), paymentTransaction.getCurrency());
        }
        assertNotNull(paymentTransactionInfoPlugin.getCreatedDate());
        assertNotNull(paymentTransactionInfoPlugin.getEffectiveDate());

        final List<PaymentPluginStatus> expectedPaymentPluginStatus;
        switch (paymentTransaction.getTransactionType()) {
            case PURCHASE:
            case AUTHORIZE:
                expectedPaymentPluginStatus = authorizedProcessed
                                              ? ImmutableList.of(PaymentPluginStatus.PROCESSED)
                                              : ImmutableList.of(PaymentPluginStatus.PROCESSED, PaymentPluginStatus.PENDING);
                break;
            case CAPTURE:
                expectedPaymentPluginStatus = ImmutableList.of(PaymentPluginStatus.PENDING);
                break;
            case REFUND:
                expectedPaymentPluginStatus = ImmutableList.of(PaymentPluginStatus.PENDING);
                break;
            case VOID:
                expectedPaymentPluginStatus = ImmutableList.of(PaymentPluginStatus.PENDING);
                break;
            default:
                expectedPaymentPluginStatus = ImmutableList.of(PaymentPluginStatus.PENDING);
                break;
        }

        if ("skip_gw".equals(paymentTransactionInfoPlugin.getGatewayError()) ||
            "true".equals(PluginProperties.findPluginPropertyValue("skipGw", paymentTransactionInfoPlugin.getProperties()))) {
            assertNull(paymentTransactionInfoPlugin.getGatewayErrorCode());
            assertEquals(paymentTransactionInfoPlugin.getStatus(), PaymentPluginStatus.PROCESSED);
            assertNull(paymentTransactionInfoPlugin.getFirstPaymentReferenceId());
        } else {
            assertNull(paymentTransactionInfoPlugin.getGatewayErrorCode());
            assertTrue(expectedPaymentPluginStatus.contains(paymentTransactionInfoPlugin.getStatus()), "was: " + paymentTransactionInfoPlugin.getStatus());

            assertNull(paymentTransactionInfoPlugin.getGatewayError());
            assertNotNull(paymentTransactionInfoPlugin.getFirstPaymentReferenceId());
            // NULL for subsequent transactions (modifications)
            //Assert.assertNotNull(paymentTransactionInfoPlugin.getSecondPaymentReferenceId());
        }
    }

    private PaymentMethodPlugin adyenEmptyPaymentMethodPlugin() {
        return adyenPaymentMethodPlugin(account.getPaymentMethodId().toString(), null);
    }

    private PaymentMethodPlugin adyenPaymentMethodPluginSepaDirectDebit() {
        return adyenPaymentMethodPlugin(account.getPaymentMethodId().toString(), "{"
                                                                                 + '"' + PROPERTY_DD_HOLDER_NAME + "\":\"" + DD_HOLDER_NAME + "\","
                                                                                 + '"' + PROPERTY_DD_ACCOUNT_NUMBER + "\":\"" + DD_IBAN + "\","
                                                                                 + '"' + PROPERTY_DD_BANK_IDENTIFIER_CODE + "\":\"" + DD_BIC + '"'
                                                                                 + '}');
    }

    private PaymentMethodPlugin adyenPaymentMethodPluginELV() {
        return adyenPaymentMethodPlugin(account.getPaymentMethodId().toString(), "{"
                                                                                 + '"' + PROPERTY_DD_HOLDER_NAME + "\":\"" + DD_HOLDER_NAME + "\","
                                                                                 + '"' + PROPERTY_DD_ACCOUNT_NUMBER + "\":\"" + ELV_KONTONUMMER + "\","
                                                                                 + '"' + PROPERTY_ELV_BLZ + "\":\"" + ELV_BANKLEITZAHL + '"'
                                                                                 + '}');
    }

    private static PaymentMethodPlugin adyenPaymentMethodPlugin(final String paymentMethodId, final String additionalData) {
        final AdyenPaymentMethodsRecord record = new AdyenPaymentMethodsRecord();
        record.setKbPaymentMethodId(paymentMethodId);
        record.setIsDefault(AdyenDao.TRUE);
        if (!Strings.isNullOrEmpty(additionalData)) {
            record.setAdditionalData(additionalData);
        }
        return new AdyenPaymentMethodPlugin(record);
    }

    private interface PluginCall {

        PaymentTransactionInfoPlugin execute(Payment payment, PaymentTransaction paymentTransaction, Iterable<PluginProperty> pluginProperties) throws PaymentPluginApiException;
    }
}
