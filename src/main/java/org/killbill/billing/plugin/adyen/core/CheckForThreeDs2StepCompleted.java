/*
 * Copyright 2014-2019 Groupon, Inc
 * Copyright 2014-2019 The Billing Project, LLC
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

import java.util.List;
import java.util.UUID;

import org.killbill.billing.catalog.api.Currency;
import org.killbill.billing.osgi.libs.killbill.OSGIKillbillAPI;
import org.killbill.billing.payment.api.Payment;
import org.killbill.billing.payment.api.PaymentApiException;
import org.killbill.billing.payment.api.PaymentTransaction;
import org.killbill.billing.payment.api.PluginProperty;
import org.killbill.billing.payment.plugin.api.PaymentPluginStatus;
import org.killbill.billing.payment.plugin.api.PaymentTransactionInfoPlugin;
import org.killbill.billing.plugin.adyen.api.AdyenCallContext;
import org.killbill.billing.plugin.adyen.api.AdyenPaymentPluginApi;
import org.killbill.billing.plugin.adyen.api.AdyenPaymentTransactionInfoPlugin;
import org.killbill.billing.plugin.adyen.api.mapping.AdyenPaymentTransaction;
import org.killbill.billing.plugin.adyen.client.AdyenConfigProperties;
import org.killbill.billing.plugin.adyen.client.model.PaymentServiceProviderResult;
import org.killbill.billing.plugin.adyen.dao.AdyenDao;
import org.killbill.billing.plugin.adyen.dao.gen.tables.records.AdyenResponsesRecord;
import org.killbill.billing.plugin.api.core.PaymentApiWrapper;
import org.killbill.billing.util.callcontext.CallContext;
import org.killbill.billing.util.callcontext.TenantContext;
import org.killbill.clock.Clock;
import org.killbill.clock.DefaultClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;

public abstract class CheckForThreeDs2StepCompleted extends DelayedActionEvent {
    private static final Logger logger = LoggerFactory.getLogger(CheckForThreeDs2StepCompleted.class);

    private final UUID kbTenantId;
    private final UUID kbPaymentMethodId;
    private final UUID kbPaymentId;
    private final UUID kbPaymentTransactionId;
    private final String kbPaymentTransactionExternalKey;
    private final String rbacUsername;
    private final String rbacPassword;

    public CheckForThreeDs2StepCompleted(final UUID uuidKey,
                                         final UUID kbTenantId,
                                         final UUID kbPaymentMethodId,
                                         final UUID kbPaymentId,
                                         final UUID kbPaymentTransactionId,
                                         final String kbPaymentTransactionExternalKey,
                                         final String rbacUsername,
                                         final String rbacPassword) {
        super(uuidKey);
        this.kbTenantId = kbTenantId;
        this.kbPaymentMethodId = kbPaymentMethodId;
        this.kbPaymentId = kbPaymentId;
        this.kbPaymentTransactionId = kbPaymentTransactionId;
        this.kbPaymentTransactionExternalKey = kbPaymentTransactionExternalKey;
        this.rbacUsername = rbacUsername;
        this.rbacPassword = rbacPassword;
    }

    public UUID getKbTenantId() {
        return kbTenantId;
    }

    public UUID getKbPaymentMethodId() {
        return kbPaymentMethodId;
    }

    public UUID getKbPaymentId() {
        return kbPaymentId;
    }

    public UUID getKbPaymentTransactionId() {
        return kbPaymentTransactionId;
    }

    public String getKbPaymentTransactionExternalKey() {
        return kbPaymentTransactionExternalKey;
    }

    public String getRbacUsername() {
        return rbacUsername;
    }

    public String getRbacPassword() {
        return rbacPassword;
    }

    private Payment getPayment(final OSGIKillbillAPI osgiKillbillAPI, final TenantContext context) {
        try {
            return osgiKillbillAPI.getPaymentApi().getPayment(kbPaymentId, true, false, ImmutableList.<PluginProperty>of(), context);
        } catch (final PaymentApiException e) {
            // Have Adyen retry
            throw new RuntimeException(String.format("Failed to retrieve kbPaymentId='%s'", kbPaymentId), e);
        }
    }

    private PaymentApiWrapper getPaymentApiWrapper(final OSGIKillbillAPI osgiKillbillAPI, final AdyenConfigProperties tenantConfiguration) {
        final Boolean invoicePaymentEnabled = tenantConfiguration != null && tenantConfiguration.getInvoicePaymentEnabled();
        return new PaymentApiWrapper(osgiKillbillAPI, invoicePaymentEnabled);
    }

    protected void performAction(final String targetState,
                                 final List<PluginProperty> extraProperties,
                                 final String errorMsg,
                                 final AdyenPaymentPluginApi adyenPaymentPluginApi,
                                 final AdyenDao adyenDao,
                                 final OSGIKillbillAPI osgiKillbillAPI,
                                 final AdyenConfigPropertiesConfigurationHandler adyenConfigPropertiesConfigurationHandler) throws Exception {
        logger.info("Checking whether state {} for payment {} has been completed", targetState, getKbPaymentId());

        final AdyenResponsesRecord previousResponse = adyenDao.getSuccessfulAuthorizationResponse(getKbPaymentId(), getKbTenantId());
        if (previousResponse == null) {
            return;
        }

        if (!previousResponse.getKbPaymentTransactionId().equals(getKbPaymentTransactionId().toString())) {
            // the payment has already advanced, so nothing to do
            return;
        }
        if (!targetState.equals(previousResponse.getResultCode())) {
            // we are not actually in the target state, so nothing to do
            return;
        }

        final UUID kbAccountId = UUID.fromString(previousResponse.getKbAccountId());
        final Clock clock = new DefaultClock();
        final CallContext context = new AdyenCallContext(clock.getUTCNow(), kbAccountId, getKbTenantId());

        // Tell Adyen about the failed notification
        logger.info("Cancelling state {} for payment {} with Adyen", targetState, getKbPaymentId());

        final PaymentTransactionInfoPlugin transaction = adyenPaymentPluginApi.authorizePayment(
                kbAccountId,
                getKbPaymentId(),
                getKbPaymentTransactionId(),
                getKbPaymentMethodId(),
                previousResponse.getAmount(),
                Currency.fromCode(previousResponse.getCurrency()),
                extraProperties,
                context);

        final AdyenConfigProperties tenantConfiguration = adyenConfigPropertiesConfigurationHandler.getConfigurable(context.getTenantId());
        final PaymentApiWrapper paymentApiWrapper = getPaymentApiWrapper(osgiKillbillAPI, tenantConfiguration);
        final Payment payment = getPayment(osgiKillbillAPI, context);
        final PaymentTransaction paymentTransaction = PaymentApiWrapper.filterForTransaction(payment, transaction.getKbTransactionPaymentId());
        final PaymentTransaction updatedPaymentTransaction = new AdyenPaymentTransaction(
                "Error",
                errorMsg,
                paymentTransaction);
        PaymentPluginStatus targetStatus = PaymentPluginStatus.ERROR;

        if (transaction instanceof AdyenPaymentTransactionInfoPlugin &&
            ((AdyenPaymentTransactionInfoPlugin)transaction).getAdyenResponseRecord().isPresent()) {
            AdyenResponsesRecord responsesRecord = ((AdyenPaymentTransactionInfoPlugin) transaction).getAdyenResponseRecord().get();

            // It is possible that the transaction was moved into authorized
            if (PaymentServiceProviderResult.AUTHORISED.getResponses()[0].equals(responsesRecord.getResultCode())) {
                targetStatus = PaymentPluginStatus.PROCESSED;
            }
        }
        advancePaymentTransaction(paymentApiWrapper, osgiKillbillAPI, payment, targetStatus, updatedPaymentTransaction, context);
    }

    private void advancePaymentTransaction(
            final PaymentApiWrapper paymentApiWrapper,
            final OSGIKillbillAPI osgiKillbillAPI,
            final Payment payment,
            final PaymentPluginStatus newStatus,
            final PaymentTransaction updatedPaymentTransaction,
            final CallContext context) throws Exception {
        try {
            osgiKillbillAPI.getSecurityApi().login(rbacUsername, rbacPassword);
            paymentApiWrapper.fixPaymentTransactionState(payment, newStatus, updatedPaymentTransaction, context);
        } finally {
            osgiKillbillAPI.getSecurityApi().logout();
        }
    }

}
