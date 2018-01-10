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

package org.killbill.billing.plugin.adyen.dao;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.annotation.Nullable;
import javax.sql.DataSource;

import org.joda.time.DateTime;
import org.jooq.SQLDialect;
import org.jooq.UpdateSetMoreStep;
import org.jooq.conf.MappedSchema;
import org.jooq.conf.RenderNameStyle;
import org.jooq.impl.DSL;
import org.killbill.billing.catalog.api.Currency;
import org.killbill.billing.payment.api.PluginProperty;
import org.killbill.billing.payment.api.TransactionType;
import org.killbill.billing.plugin.adyen.api.AdyenPaymentPluginApi;
import org.killbill.billing.plugin.adyen.client.AdyenConfigProperties;
import org.killbill.billing.plugin.adyen.client.model.NotificationItem;
import org.killbill.billing.plugin.adyen.client.model.PaymentModificationResponse;
import org.killbill.billing.plugin.adyen.client.model.PaymentServiceProviderResult;
import org.killbill.billing.plugin.adyen.client.model.PurchaseResult;
import org.killbill.billing.plugin.adyen.dao.gen.tables.AdyenPaymentMethods;
import org.killbill.billing.plugin.adyen.dao.gen.tables.AdyenResponses;
import org.killbill.billing.plugin.adyen.dao.gen.tables.records.AdyenHppRequestsRecord;
import org.killbill.billing.plugin.adyen.dao.gen.tables.records.AdyenNotificationsRecord;
import org.killbill.billing.plugin.adyen.dao.gen.tables.records.AdyenPaymentMethodsRecord;
import org.killbill.billing.plugin.adyen.dao.gen.tables.records.AdyenResponsesRecord;
import org.killbill.billing.plugin.api.PluginProperties;
import org.killbill.billing.plugin.api.payment.PluginPaymentPluginApi;
import org.killbill.billing.plugin.dao.payment.PluginPaymentDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import static org.killbill.billing.plugin.adyen.api.AdyenPaymentPluginApi.PROPERTY_PSP_REFERENCE;
import static org.killbill.billing.plugin.adyen.client.model.PurchaseResult.ADYEN_CALL_ERROR_STATUS;
import static org.killbill.billing.plugin.adyen.client.model.PurchaseResult.EXCEPTION_CLASS;
import static org.killbill.billing.plugin.adyen.client.model.PurchaseResult.EXCEPTION_MESSAGE;
import static org.killbill.billing.plugin.adyen.dao.gen.tables.AdyenHppRequests.ADYEN_HPP_REQUESTS;
import static org.killbill.billing.plugin.adyen.dao.gen.tables.AdyenNotifications.ADYEN_NOTIFICATIONS;
import static org.killbill.billing.plugin.adyen.dao.gen.tables.AdyenPaymentMethods.ADYEN_PAYMENT_METHODS;
import static org.killbill.billing.plugin.adyen.dao.gen.tables.AdyenResponses.ADYEN_RESPONSES;

public class AdyenDao extends PluginPaymentDao<AdyenResponsesRecord, AdyenResponses, AdyenPaymentMethodsRecord, AdyenPaymentMethods> {

    private static final Logger logger = LoggerFactory.getLogger(AdyenDao.class);

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Joiner JOINER = Joiner.on(",");

    public AdyenDao(final DataSource dataSource) throws SQLException {
        super(AdyenResponses.ADYEN_RESPONSES, AdyenPaymentMethods.ADYEN_PAYMENT_METHODS, dataSource);
    }

    public AdyenDao(DataSource dataSource, SQLDialect dialect) throws SQLException {
        super(AdyenResponses.ADYEN_RESPONSES, AdyenPaymentMethods.ADYEN_PAYMENT_METHODS, dataSource, dialect);
    }

    public AdyenDao(DataSource dataSource, SQLDialect dialect, AdyenConfigProperties configProperties) throws SQLException {
        super(AdyenResponses.ADYEN_RESPONSES, AdyenPaymentMethods.ADYEN_PAYMENT_METHODS, dataSource, dialect);
        overrideSettings(configProperties);
    }


    void overrideSettings(AdyenConfigProperties configProperties) {

        String targetSchema = configProperties.getSchema();
        if (null != targetSchema && targetSchema.trim().length() > 0) {
            List<MappedSchema> schemas= settings.getRenderMapping().getSchemata();
            for (MappedSchema schema: schemas) {
                if (schema.getInput().equals(DEFAULT_SCHEMA_NAME)) {
                    schema.setOutput(targetSchema);
                }
                logger.info("Output schema overridden by configuration.  input: '{}' mapped to output: '{}'", schema.getInput(), schema.getOutput());
            }
        } else {
            logger.info("Default schema mapping used");
        }

        String renderCatalogProp = configProperties.getRenderCatalog();
        if (null != renderCatalogProp && renderCatalogProp.trim().length() > 0) {
            boolean renderCatalog = Boolean.parseBoolean(renderCatalogProp);
            settings.setRenderCatalog(renderCatalog);
            logger.info("Setting renderCatalog overridden.  value: '{}'", renderCatalog);
        }

        String renderSchemaProp = configProperties.getRenderSchema();
        if (null != renderSchemaProp && renderSchemaProp.trim().length() > 0) {
            boolean renderSchema = Boolean.parseBoolean(renderSchemaProp);
            settings.setRenderCatalog(renderSchema);
            logger.info("Setting renderSchema overridden.  value: '{}'", renderSchema);
        }
    }

    // Payment methods

    public void setPaymentMethodToken(final String kbPaymentMethodId, final String token, final String kbTenantId) throws SQLException {
        execute(dataSource.getConnection(),
                new WithConnectionCallback<AdyenResponsesRecord>() {
                    @Override
                    public AdyenResponsesRecord withConnection(final Connection conn) throws SQLException {
                        DSL.using(conn, dialect, settings)
                           .update(ADYEN_PAYMENT_METHODS)
                           .set(ADYEN_PAYMENT_METHODS.TOKEN, token)
                           .where(ADYEN_PAYMENT_METHODS.KB_PAYMENT_METHOD_ID.equal(kbPaymentMethodId))
                           .and(ADYEN_PAYMENT_METHODS.KB_TENANT_ID.equal(kbTenantId))
                           .and(ADYEN_PAYMENT_METHODS.IS_DELETED.equal(FALSE))
                           .execute();
                        return null;
                    }
                });
    }

    @Override
    public void addPaymentMethod(final UUID kbAccountId, final UUID kbPaymentMethodId, final boolean isDefault,
                                 final Map<String, String> properties, final DateTime utcNow, final UUID kbTenantId) throws SQLException {

        if (!dialect.equals(SQLDialect.POSTGRES)) {
            super.addPaymentMethod(kbAccountId, kbPaymentMethodId, isDefault, properties, utcNow, kbTenantId);
            return;
        }

        addPaymentMethodWithoutTableNames(kbAccountId, kbPaymentMethodId, isDefault, properties, utcNow, kbTenantId);
    }

    protected void addPaymentMethodWithoutTableNames(final UUID kbAccountId, final UUID kbPaymentMethodId, final boolean isDefault,
                                                     final Map<String, String> properties, final DateTime utcNow, final UUID kbTenantId) throws SQLException {

        /* Clone our properties, what we have been given might be unmodifiable */
        final Map<String, String> clonedProperties = new HashMap<String, String>(properties);

        /* Extract and remove known values from the properties map that will become "additional data" */
        final String token               = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_TOKEN);
        final String ccFirstName         = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_FIRST_NAME);
        final String ccLastName          = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_LAST_NAME);
        final String ccType              = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_TYPE);
        final String ccExpirationMonth   = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_EXPIRATION_MONTH);
        final String ccExpirationYear    = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_EXPIRATION_YEAR);
        final String ccNumber            = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_NUMBER);
        final String ccStartMonth        = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_START_MONTH);
        final String ccStartYear         = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_START_YEAR);
        final String ccIssueNumber       = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_ISSUE_NUMBER);
        final String ccVerificationValue = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_VERIFICATION_VALUE);
        final String ccTrackData         = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CC_TRACK_DATA);
        final String address1            = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_ADDRESS1);
        final String address2            = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_ADDRESS2);
        final String city                = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_CITY);
        final String state               = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_STATE);
        final String zip                 = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_ZIP);
        final String country             = clonedProperties.remove(PluginPaymentPluginApi.PROPERTY_COUNTRY);

        /* Calculate last 4 digits of the credit card number */
        final String ccLast4 = ccNumber == null ? null : ccNumber.substring(ccNumber.length() - 4, ccNumber.length());

        /* Calculate the additional data to store */
        final String additionalData = asString(clonedProperties);

        /* Store computed data */
        execute(dataSource.getConnection(),
                new WithConnectionCallback<Void>() {
                    @Override
                    public Void withConnection(final Connection conn) throws SQLException {
                        DSL.using(conn, dialect, settings)
                           .insertInto(paymentMethodsTable,
                                       DSL.field(KB_ACCOUNT_ID),
                                       DSL.field(KB_PAYMENT_METHOD_ID),
                                       DSL.field(TOKEN),
                                       DSL.field(CC_FIRST_NAME),
                                       DSL.field(CC_LAST_NAME),
                                       DSL.field(CC_TYPE),
                                       DSL.field(CC_EXP_MONTH),
                                       DSL.field(CC_EXP_YEAR),
                                       DSL.field(CC_NUMBER),
                                       DSL.field(CC_LAST_4),
                                       DSL.field(CC_START_MONTH),
                                       DSL.field(CC_START_YEAR),
                                       DSL.field(CC_ISSUE_NUMBER),
                                       DSL.field(CC_VERIFICATION_VALUE),
                                       DSL.field(CC_TRACK_DATA),
                                       DSL.field(ADDRESS1),
                                       DSL.field(ADDRESS2),
                                       DSL.field(CITY),
                                       DSL.field(STATE),
                                       DSL.field(ZIP),
                                       DSL.field(COUNTRY),
                                       DSL.field(IS_DEFAULT),
                                       DSL.field(IS_DELETED),
                                       DSL.field(ADDITIONAL_DATA),
                                       DSL.field(CREATED_DATE),
                                       DSL.field(UPDATED_DATE),
                                       DSL.field(KB_TENANT_ID))
                           .values(kbAccountId.toString(),
                                   kbPaymentMethodId.toString(),
                                   token,
                                   ccFirstName,
                                   ccLastName,
                                   ccType,
                                   ccExpirationMonth,
                                   ccExpirationYear,
                                   ccNumber,
                                   ccLast4,
                                   ccStartMonth,
                                   ccStartYear,
                                   ccIssueNumber,
                                   ccVerificationValue,
                                   ccTrackData,
                                   address1,
                                   address2,
                                   city,
                                   state,
                                   zip,
                                   country,
                                   isDefault ? TRUE : FALSE,
                                   FALSE,
                                   additionalData,
                                   toTimestamp(utcNow),
                                   toTimestamp(utcNow),
                                   kbTenantId.toString())
                           .execute();
                        return null;
                    }
                });
    }


    // HPP requests

    public void addHppRequest(final UUID kbAccountId,
                              @Nullable final UUID kbPaymentId,
                              @Nullable final UUID kbPaymentTransactionId,
                              final String transactionExternalKey,
                              final Map additionalDataMap,
                              final DateTime utcNow,
                              final UUID kbTenantId) throws SQLException {
        final String additionalData = asString(additionalDataMap);

        execute(dataSource.getConnection(),
                new WithConnectionCallback<Void>() {
                    @Override
                    public Void withConnection(final Connection conn) throws SQLException {
                        DSL.using(conn, dialect, settings)
                           .insertInto(ADYEN_HPP_REQUESTS,
                                       ADYEN_HPP_REQUESTS.KB_ACCOUNT_ID,
                                       ADYEN_HPP_REQUESTS.KB_PAYMENT_ID,
                                       ADYEN_HPP_REQUESTS.KB_PAYMENT_TRANSACTION_ID,
                                       ADYEN_HPP_REQUESTS.TRANSACTION_EXTERNAL_KEY,
                                       ADYEN_HPP_REQUESTS.ADDITIONAL_DATA,
                                       ADYEN_HPP_REQUESTS.CREATED_DATE,
                                       ADYEN_HPP_REQUESTS.KB_TENANT_ID)
                           .values(kbAccountId.toString(),
                                   kbPaymentId != null ? kbPaymentId.toString() : null,
                                   kbPaymentTransactionId != null ? kbPaymentTransactionId.toString() : null,
                                   transactionExternalKey,
                                   additionalData,
                                   toTimestamp(utcNow),
                                   kbTenantId.toString())
                           .execute();
                        return null;
                    }
                });
    }

    public AdyenHppRequestsRecord getHppRequest(final String merchantReference) throws SQLException {
        return execute(dataSource.getConnection(),
                       new WithConnectionCallback<AdyenHppRequestsRecord>() {
                           @Override
                           public AdyenHppRequestsRecord withConnection(final Connection conn) throws SQLException {
                               return DSL.using(conn, dialect, settings)
                                         .selectFrom(ADYEN_HPP_REQUESTS)
                                         .where(ADYEN_HPP_REQUESTS.TRANSACTION_EXTERNAL_KEY.equal(merchantReference))
                                         .orderBy(ADYEN_HPP_REQUESTS.RECORD_ID.desc())
                                         .limit(1)
                                         .fetchOne();
                           }
                       });
    }

    public AdyenHppRequestsRecord getHppRequest(final UUID kbPaymentTransactionId) throws SQLException {
        return execute(dataSource.getConnection(),
                       new WithConnectionCallback<AdyenHppRequestsRecord>() {
                           @Override
                           public AdyenHppRequestsRecord withConnection(final Connection conn) throws SQLException {
                               return DSL.using(conn, dialect, settings)
                                         .selectFrom(ADYEN_HPP_REQUESTS)
                                         .where(ADYEN_HPP_REQUESTS.KB_PAYMENT_TRANSACTION_ID.equal(kbPaymentTransactionId.toString()))
                                         .orderBy(ADYEN_HPP_REQUESTS.RECORD_ID.desc())
                                         .limit(1)
                                         .fetchOne();
                           }
                       });
    }

    // Responses

    public AdyenResponsesRecord addResponse(final UUID kbAccountId,
                                            final UUID kbPaymentId,
                                            final UUID kbPaymentTransactionId,
                                            final TransactionType transactionType,
                                            final BigDecimal amount,
                                            final Currency currency,
                                            final PurchaseResult result,
                                            final DateTime utcNow,
                                            final UUID kbTenantId) throws SQLException {
        final String dccAmountValue = getProperty(AdyenPaymentPluginApi.PROPERTY_DCC_AMOUNT_VALUE, result);
        final String additionalData = getAdditionalData(result);

        return execute(dataSource.getConnection(),
                       new WithConnectionCallback<AdyenResponsesRecord>() {
                           @Override
                           public AdyenResponsesRecord withConnection(final Connection conn) throws SQLException {
                               DSL.using(conn, dialect, settings)
                                  .insertInto(ADYEN_RESPONSES,
                                              ADYEN_RESPONSES.KB_ACCOUNT_ID,
                                              ADYEN_RESPONSES.KB_PAYMENT_ID,
                                              ADYEN_RESPONSES.KB_PAYMENT_TRANSACTION_ID,
                                              ADYEN_RESPONSES.TRANSACTION_TYPE,
                                              ADYEN_RESPONSES.AMOUNT,
                                              ADYEN_RESPONSES.CURRENCY,
                                              ADYEN_RESPONSES.PSP_RESULT,
                                              ADYEN_RESPONSES.PSP_REFERENCE,
                                              ADYEN_RESPONSES.AUTH_CODE,
                                              ADYEN_RESPONSES.RESULT_CODE,
                                              ADYEN_RESPONSES.REFUSAL_REASON,
                                              ADYEN_RESPONSES.REFERENCE,
                                              ADYEN_RESPONSES.PSP_ERROR_CODES,
                                              ADYEN_RESPONSES.PAYMENT_INTERNAL_REF,
                                              ADYEN_RESPONSES.FORM_URL,
                                              ADYEN_RESPONSES.DCC_AMOUNT,
                                              ADYEN_RESPONSES.DCC_CURRENCY,
                                              ADYEN_RESPONSES.DCC_SIGNATURE,
                                              ADYEN_RESPONSES.ISSUER_URL,
                                              ADYEN_RESPONSES.MD,
                                              ADYEN_RESPONSES.PA_REQUEST,
                                              ADYEN_RESPONSES.ADDITIONAL_DATA,
                                              ADYEN_RESPONSES.CREATED_DATE,
                                              ADYEN_RESPONSES.KB_TENANT_ID)
                                  .values(kbAccountId.toString(),
                                          kbPaymentId.toString(),
                                          kbPaymentTransactionId.toString(),
                                          transactionType.toString(),
                                          amount,
                                          currency,
                                          result.getResult().isPresent() ? result.getResult().get().toString() : null,
                                          result.getPspReference(),
                                          result.getAuthCode(),
                                          result.getResultCode(),
                                          result.getReason(),
                                          result.getReference(),
                                          null,
                                          result.getPaymentTransactionExternalKey(),
                                          result.getFormUrl(),
                                          dccAmountValue == null ? null : new BigDecimal(dccAmountValue),
                                          getProperty(AdyenPaymentPluginApi.PROPERTY_DCC_AMOUNT_CURRENCY, result),
                                          getProperty(AdyenPaymentPluginApi.PROPERTY_DCC_SIGNATURE, result),
                                          getProperty(AdyenPaymentPluginApi.PROPERTY_ISSUER_URL, result),
                                          getProperty(AdyenPaymentPluginApi.PROPERTY_MD, result),
                                          getProperty(AdyenPaymentPluginApi.PROPERTY_PA_REQ, result),
                                          additionalData,
                                          toTimestamp(utcNow),
                                          kbTenantId.toString())
                                  .execute();

                               return DSL.using(conn, dialect, settings)
                                         .selectFrom(ADYEN_RESPONSES)
                                         .where(ADYEN_RESPONSES.KB_PAYMENT_TRANSACTION_ID.equal(kbPaymentTransactionId.toString()))
                                         .and(ADYEN_RESPONSES.KB_TENANT_ID.equal(kbTenantId.toString()))
                                         .orderBy(ADYEN_RESPONSES.RECORD_ID.desc())
                                         .limit(1)
                                         .fetchOne();
                           }
                       });
    }

    public void addResponse(final UUID kbAccountId,
                            final UUID kbPaymentId,
                            final UUID kbPaymentTransactionId,
                            final TransactionType transactionType,
                            @Nullable final BigDecimal amount,
                            @Nullable final Currency currency,
                            final PaymentModificationResponse result,
                            final DateTime utcNow,
                            final UUID kbTenantId) throws SQLException {
        final String dccAmountValue = getProperty(AdyenPaymentPluginApi.PROPERTY_DCC_AMOUNT_VALUE, result);
        final String additionalData = getAdditionalData(result);

        execute(dataSource.getConnection(),
                new WithConnectionCallback<Void>() {
                    @Override
                    public Void withConnection(final Connection conn) throws SQLException {
                        DSL.using(conn, dialect, settings)
                           .insertInto(ADYEN_RESPONSES,
                                       ADYEN_RESPONSES.KB_ACCOUNT_ID,
                                       ADYEN_RESPONSES.KB_PAYMENT_ID,
                                       ADYEN_RESPONSES.KB_PAYMENT_TRANSACTION_ID,
                                       ADYEN_RESPONSES.TRANSACTION_TYPE,
                                       ADYEN_RESPONSES.AMOUNT,
                                       ADYEN_RESPONSES.CURRENCY,
                                       ADYEN_RESPONSES.PSP_RESULT,
                                       ADYEN_RESPONSES.PSP_REFERENCE,
                                       ADYEN_RESPONSES.AUTH_CODE,
                                       ADYEN_RESPONSES.RESULT_CODE,
                                       ADYEN_RESPONSES.REFUSAL_REASON,
                                       ADYEN_RESPONSES.REFERENCE,
                                       ADYEN_RESPONSES.PSP_ERROR_CODES,
                                       ADYEN_RESPONSES.PAYMENT_INTERNAL_REF,
                                       ADYEN_RESPONSES.FORM_URL,
                                       ADYEN_RESPONSES.DCC_AMOUNT,
                                       ADYEN_RESPONSES.DCC_CURRENCY,
                                       ADYEN_RESPONSES.DCC_SIGNATURE,
                                       ADYEN_RESPONSES.ISSUER_URL,
                                       ADYEN_RESPONSES.MD,
                                       ADYEN_RESPONSES.PA_REQUEST,
                                       ADYEN_RESPONSES.ADDITIONAL_DATA,
                                       ADYEN_RESPONSES.CREATED_DATE,
                                       ADYEN_RESPONSES.KB_TENANT_ID)
                           .values(kbAccountId.toString(),
                                   kbPaymentId.toString(),
                                   kbPaymentTransactionId.toString(),
                                   transactionType.toString(),
                                   amount,
                                   currency,
                                   result.getResponse(),
                                   result.getPspReference(),
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   null,
                                   dccAmountValue == null ? null : new BigDecimal(dccAmountValue),
                                   getProperty(AdyenPaymentPluginApi.PROPERTY_DCC_AMOUNT_CURRENCY, result),
                                   getProperty(AdyenPaymentPluginApi.PROPERTY_DCC_SIGNATURE, result),
                                   getProperty(AdyenPaymentPluginApi.PROPERTY_ISSUER_URL, result),
                                   getProperty(AdyenPaymentPluginApi.PROPERTY_MD, result),
                                   getProperty(AdyenPaymentPluginApi.PROPERTY_PA_REQ, result),
                                   additionalData,
                                   toTimestamp(utcNow),
                                   kbTenantId.toString())
                           .execute();
                        return null;
                    }
                });
    }

    public AdyenResponsesRecord updateResponse(final UUID kbPaymentTransactionId, final Iterable<PluginProperty> additionalPluginProperties, final UUID kbTenantId) throws SQLException {
        return updateResponse(kbPaymentTransactionId, null, additionalPluginProperties, kbTenantId);
    }

    /**
     * Update the PSP reference and additional data of the latest response row for a payment transaction
     *
     * @param kbPaymentTransactionId       Kill Bill payment transaction id
     * @param paymentServiceProviderResult New PSP result (null if unchanged)
     * @param additionalPluginProperties   Latest properties
     * @param kbTenantId                   Kill Bill tenant id
     * @return the latest version of the response row, null if one couldn't be found
     * @throws SQLException For any unexpected SQL error
     */
    public AdyenResponsesRecord updateResponse(final UUID kbPaymentTransactionId, @Nullable final PaymentServiceProviderResult paymentServiceProviderResult, final Iterable<PluginProperty> additionalPluginProperties, final UUID kbTenantId) throws SQLException {
        final Map<String, Object> additionalProperties = PluginProperties.toMap(additionalPluginProperties);

        return execute(dataSource.getConnection(),
                       new WithConnectionCallback<AdyenResponsesRecord>() {
                           @Override
                           public AdyenResponsesRecord withConnection(final Connection conn) throws SQLException {
                               final AdyenResponsesRecord response = DSL.using(conn, dialect, settings)
                                                                        .selectFrom(ADYEN_RESPONSES)
                                                                        .where(ADYEN_RESPONSES.KB_PAYMENT_TRANSACTION_ID.equal(kbPaymentTransactionId.toString()))
                                                                        .and(ADYEN_RESPONSES.KB_TENANT_ID.equal(kbTenantId.toString()))
                                                                        .orderBy(ADYEN_RESPONSES.RECORD_ID.desc())
                                                                        .limit(1)
                                                                        .fetchOne();

                               if (response == null) {
                                   return null;
                               }

                               final Map originalData = new HashMap(fromAdditionalData(response.getAdditionalData()));
                               originalData.putAll(additionalProperties);
                               final String pspReference = getProperty(PROPERTY_PSP_REFERENCE, additionalProperties);
                               if (pspReference != null) {
                                   // If there is a PSP reference, the call went eventually to Adyen. Remove exceptions
                                   originalData.remove(ADYEN_CALL_ERROR_STATUS);
                                   originalData.remove(EXCEPTION_CLASS);
                                   originalData.remove(EXCEPTION_MESSAGE);
                               }
                               final String mergedAdditionalData = asString(originalData);

                               UpdateSetMoreStep<AdyenResponsesRecord> step = DSL.using(conn, dialect, settings)
                                                                                 .update(ADYEN_RESPONSES)
                                                                                 .set(ADYEN_RESPONSES.PSP_REFERENCE, pspReference)
                                                                                 .set(ADYEN_RESPONSES.ADDITIONAL_DATA, mergedAdditionalData);
                               if (paymentServiceProviderResult != null) {
                                   step = step.set(ADYEN_RESPONSES.PSP_RESULT, paymentServiceProviderResult.toString());
                               }
                               step.where(ADYEN_RESPONSES.RECORD_ID.equal(response.getRecordId()))
                                   .execute();

                               return DSL.using(conn, dialect, settings)
                                         .selectFrom(ADYEN_RESPONSES)
                                         .where(ADYEN_RESPONSES.KB_PAYMENT_TRANSACTION_ID.equal(kbPaymentTransactionId.toString()))
                                         .and(ADYEN_RESPONSES.KB_TENANT_ID.equal(kbTenantId.toString()))
                                         .orderBy(ADYEN_RESPONSES.RECORD_ID.desc())
                                         .limit(1)
                                         .fetchOne();
                           }
                       });
    }

    @Override
    public List<AdyenResponsesRecord> getResponses(final UUID kbPaymentId, final UUID kbTenantId) throws SQLException {
        final List<AdyenResponsesRecord> responses = new LinkedList<AdyenResponsesRecord>();
        for (final AdyenResponsesRecord adyenResponsesRecord : Lists.<AdyenResponsesRecord>reverse(super.getResponses(kbPaymentId, kbTenantId))) {
            responses.add(adyenResponsesRecord);

            // Keep only the completion row for 3D-S
            if (TransactionType.AUTHORIZE.toString().equals(adyenResponsesRecord.getTransactionType())) {
                break;
            }
        }
        return Lists.<AdyenResponsesRecord>reverse(responses);
    }

    // Assumes that the last auth was successful
    @Override
    public AdyenResponsesRecord getSuccessfulAuthorizationResponse(final UUID kbPaymentId, final UUID kbTenantId) throws SQLException {
        return execute(dataSource.getConnection(),
                       new WithConnectionCallback<AdyenResponsesRecord>() {
                           @Override
                           public AdyenResponsesRecord withConnection(final Connection conn) throws SQLException {
                               return DSL.using(conn, dialect, settings)
                                         .selectFrom(responsesTable)
                                         .where(DSL.field(responsesTable.getName() + "." + KB_PAYMENT_ID).equal(kbPaymentId.toString()))
                                         .and(
                                                 DSL.field(responsesTable.getName() + "." + TRANSACTION_TYPE).equal(TransactionType.AUTHORIZE.toString())
                                                    .or(DSL.field(responsesTable.getName() + "." + TRANSACTION_TYPE).equal(TransactionType.PURCHASE.toString()))
                                             )
                                         .and(DSL.field(responsesTable.getName() + "." + KB_TENANT_ID).equal(kbTenantId.toString()))
                                         .orderBy(DSL.field(responsesTable.getName() + "." + RECORD_ID).desc())
                                         .limit(1)
                                         .fetchOne();
                           }
                       });
    }

    public AdyenResponsesRecord getResponse(final String pspReference) throws SQLException {
        return execute(dataSource.getConnection(),
                       new WithConnectionCallback<AdyenResponsesRecord>() {
                           @Override
                           public AdyenResponsesRecord withConnection(final Connection conn) throws SQLException {
                               return DSL.using(conn, dialect, settings)
                                         .selectFrom(ADYEN_RESPONSES)
                                         .where(ADYEN_RESPONSES.PSP_REFERENCE.equal(pspReference))
                                         .orderBy(ADYEN_RESPONSES.RECORD_ID.desc())
                                         // Can have multiple entries for 3D-S
                                         .limit(1)
                                         .fetchOne();
                           }
                       });
    }

    // Notifications

    public void addNotification(@Nullable final UUID kbAccountId,
                                @Nullable final UUID kbPaymentId,
                                @Nullable final UUID kbPaymentTransactionId,
                                @Nullable final TransactionType transactionType,
                                final NotificationItem notification,
                                final DateTime utcNow,
                                @Nullable final UUID kbTenantId) throws SQLException {
        final String additionalData = asString(notification.getAdditionalData());

        execute(dataSource.getConnection(),
                new WithConnectionCallback<Void>() {
                    @Override
                    public Void withConnection(final Connection conn) throws SQLException {
                        DSL.using(conn, dialect, settings)
                           .insertInto(ADYEN_NOTIFICATIONS,
                                       ADYEN_NOTIFICATIONS.KB_ACCOUNT_ID,
                                       ADYEN_NOTIFICATIONS.KB_PAYMENT_ID,
                                       ADYEN_NOTIFICATIONS.KB_PAYMENT_TRANSACTION_ID,
                                       ADYEN_NOTIFICATIONS.TRANSACTION_TYPE,
                                       ADYEN_NOTIFICATIONS.AMOUNT,
                                       ADYEN_NOTIFICATIONS.CURRENCY,
                                       ADYEN_NOTIFICATIONS.EVENT_CODE,
                                       ADYEN_NOTIFICATIONS.EVENT_DATE,
                                       ADYEN_NOTIFICATIONS.MERCHANT_ACCOUNT_CODE,
                                       ADYEN_NOTIFICATIONS.MERCHANT_REFERENCE,
                                       ADYEN_NOTIFICATIONS.OPERATIONS,
                                       ADYEN_NOTIFICATIONS.ORIGINAL_REFERENCE,
                                       ADYEN_NOTIFICATIONS.PAYMENT_METHOD,
                                       ADYEN_NOTIFICATIONS.PSP_REFERENCE,
                                       ADYEN_NOTIFICATIONS.REASON,
                                       ADYEN_NOTIFICATIONS.SUCCESS,
                                       ADYEN_NOTIFICATIONS.ADDITIONAL_DATA,
                                       ADYEN_NOTIFICATIONS.CREATED_DATE,
                                       ADYEN_NOTIFICATIONS.KB_TENANT_ID)
                           .values(kbAccountId == null ? null : kbAccountId.toString(),
                                   kbPaymentId == null ? null : kbPaymentId.toString(),
                                   kbPaymentTransactionId == null ? null : kbPaymentTransactionId.toString(),
                                   transactionType == null ? null : transactionType.toString(),
                                   notification.getAmount(),
                                   notification.getCurrency(),
                                   notification.getEventCode(),
                                   toTimestamp(notification.getEventDate()),
                                   notification.getMerchantAccountCode(),
                                   notification.getMerchantReference(),
                                   getString(notification.getOperations()),
                                   notification.getOriginalReference(),
                                   notification.getPaymentMethod(),
                                   notification.getPspReference(),
                                   notification.getReason(),
                                   notification.getSuccess() == null ? FALSE : fromBoolean(notification.getSuccess()),
                                   additionalData,
                                   toTimestamp(utcNow),
                                   kbTenantId == null ? null : kbTenantId.toString())
                           .execute();
                        return null;
                    }
                });
    }

    @VisibleForTesting
    AdyenNotificationsRecord getNotification(final String pspReference) throws SQLException {
        return execute(dataSource.getConnection(),
                       new WithConnectionCallback<AdyenNotificationsRecord>() {
                           @Override
                           public AdyenNotificationsRecord withConnection(final Connection conn) throws SQLException {
                               return DSL.using(conn, dialect, settings)
                                         .selectFrom(ADYEN_NOTIFICATIONS)
                                         .where(ADYEN_NOTIFICATIONS.PSP_REFERENCE.equal(pspReference))
                                         .orderBy(ADYEN_NOTIFICATIONS.RECORD_ID.desc())
                                         .limit(1)
                                         .fetchOne();
                           }
                       });
    }

    // Just for testing
    public List<AdyenNotificationsRecord> getNotifications() throws SQLException {
        return execute(dataSource.getConnection(),
                       new WithConnectionCallback<List<AdyenNotificationsRecord>>() {
                           @Override
                           public List<AdyenNotificationsRecord> withConnection(final Connection conn) throws SQLException {
                               return DSL.using(conn, dialect, settings)
                                         .selectFrom(ADYEN_NOTIFICATIONS)
                                         .orderBy(ADYEN_NOTIFICATIONS.RECORD_ID.asc())
                                         .fetch();
                           }
                       });
    }

    private String getString(@Nullable final Iterable<?> iterable) {
        if (iterable == null || !iterable.iterator().hasNext()) {
            return null;
        } else {
            return JOINER.join(Iterables.transform(iterable, Functions.toStringFunction()));
        }
    }

    private String getProperty(final String key, final PurchaseResult result) {
        return getProperty(key, result.getFormParameter());
    }

    private String getProperty(final String key, final PaymentModificationResponse response) {
        return getProperty(key, response.getAdditionalData());
    }

    private String getAdditionalData(final PurchaseResult result) throws SQLException {
        final Map<String, String> additionalDataMap = new HashMap<String, String>();
        if (result.getAdditionalData() != null && !result.getAdditionalData().isEmpty()) {
            additionalDataMap.putAll(result.getAdditionalData());
        }
        if (result.getFormParameter() != null && !result.getFormParameter().isEmpty()) {
            additionalDataMap.putAll(result.getFormParameter());
        }
        if (additionalDataMap.isEmpty()) {
            return null;
        } else {
            return asString(additionalDataMap);
        }
    }

    private String getAdditionalData(final PaymentModificationResponse response) throws SQLException {
        return asString(response.getAdditionalData());
    }

    public static Map fromAdditionalData(@Nullable final String additionalData) {
        if (additionalData == null) {
            return ImmutableMap.of();
        }

        try {
            return objectMapper.readValue(additionalData, Map.class);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }
}
