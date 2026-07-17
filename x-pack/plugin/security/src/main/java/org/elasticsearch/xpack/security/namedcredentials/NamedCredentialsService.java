/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.namedcredentials;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.namedcredentials.CredentialAuthType;
import org.elasticsearch.xpack.core.security.action.namedcredentials.NamedCredential;
import org.elasticsearch.xpack.core.security.action.namedcredentials.PutNamedCredentialAction;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * CRUD for named credentials stored in the {@code .security-named-credentials} system index.
 * The {@code auth} block of each credential is serialized, encrypted through the cluster's
 * {@link EncryptionService} (project encryption key), and stored as an opaque binary blob;
 * every read path except {@link #decryptCredential} leaves it untouched.
 */
public class NamedCredentialsService {

    private static final Logger logger = LogManager.getLogger(NamedCredentialsService.class);
    private static final int MAX_CREDENTIALS = 10_000;

    private final Client client;
    private final SecurityIndexManager indexManager;
    private final Supplier<EncryptionService> encryptionServiceSupplier;
    private final Clock clock;

    public NamedCredentialsService(
        Client client,
        SecurityIndexManager indexManager,
        Supplier<EncryptionService> encryptionServiceSupplier,
        Clock clock
    ) {
        this.client = client;
        this.indexManager = indexManager;
        this.encryptionServiceSupplier = encryptionServiceSupplier;
        this.clock = clock;
    }

    /**
     * Holds the raw document source together with sequence number metadata for optimistic concurrency.
     */
    private record RawGetResult(Map<String, Object> source, long seqNo, long primaryTerm) {}

    /**
     * Creates or replaces a named credential. Responds {@code true} if created, {@code false} if replaced.
     */
    public void putCredential(PutNamedCredentialAction.Request request, ActionListener<Boolean> listener) {
        final EncryptionService encryption = requireEncryptionService(listener);
        if (encryption == null) {
            return;
        }
        getRawGetResult(request.credentialName(), ActionListener.<RawGetResult>wrap(existing -> {
            final boolean creating = existing == null;
            final Map<String, Object> existingSource = creating ? null : existing.source();
            final String authBlob;
            if (request.auth() != null) {
                authBlob = encryptAuth(encryption, request.auth());
            } else if (creating) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "auth is required when creating named credential [{}]",
                        RestStatus.BAD_REQUEST,
                        request.credentialName()
                    )
                );
                return;
            } else if (request.authType().typeName().equals(existingSource.get("auth_type")) == false) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "auth is required when changing auth_type of named credential [{}] from [{}] to [{}]",
                        RestStatus.BAD_REQUEST,
                        request.credentialName(),
                        existingSource.get("auth_type"),
                        request.authType().typeName()
                    )
                );
                return;
            } else {
                // Carry-forward: request omitted auth, keep the stored encrypted blob as-is.
                logger.debug("carrying forward existing auth blob for named credential [{}]", request.credentialName());
                authBlob = (String) existingSource.get("auth");
            }
            final long now = clock.millis();
            final long createdAt = creating ? now : ((Number) existingSource.get("created_at")).longValue();
            // Carry-forward: if config omitted on update, keep the stored config.
            final Map<String, String> effectiveConfig;
            if (request.config() != null) {
                effectiveConfig = request.config();
            } else if (creating == false) {
                @SuppressWarnings("unchecked")
                Map<String, String> storedConfig = (Map<String, String>) existingSource.get("config");
                effectiveConfig = storedConfig != null ? storedConfig : Map.of();
            } else {
                effectiveConfig = Map.of();
            }
            final Map<String, Object> source = buildSource(request, effectiveConfig, authBlob, createdAt, now);
            final IndexRequest indexRequest = client.prepareIndex(indexManager.aliasName())
                .setId(request.credentialName())
                .setSource(source)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .request();
            if (creating) {
                indexRequest.opType(DocWriteRequest.OpType.CREATE);
            } else {
                indexRequest.setIfSeqNo(existing.seqNo()).setIfPrimaryTerm(existing.primaryTerm());
            }
            indexManager.forCurrentProject()
                .prepareIndexIfNeededThenExecute(
                    listener::onFailure,
                    () -> executeAsyncWithOrigin(
                        client,
                        SECURITY_ORIGIN,
                        TransportIndexAction.TYPE,
                        indexRequest,
                        ActionListener.<DocWriteResponse>wrap(
                            response -> listener.onResponse(response.getResult() == DocWriteResponse.Result.CREATED),
                            listener::onFailure
                        )
                    )
                );
        }, listener::onFailure));
    }

    /**
     * Returns credentials (with auth redacted). If {@code name} is non-null, returns a single-element list or
     * fails with {@link org.elasticsearch.ResourceNotFoundException} if not found.
     * If {@code name} is null and the index does not exist, returns an empty list.
     */
    public void getCredentials(@Nullable String name, ActionListener<List<NamedCredential>> listener) {
        if (name != null) {
            getRawSource(name, ActionListener.<Map<String, Object>>wrap(source -> {
                if (source == null) {
                    listener.onFailure(new org.elasticsearch.ResourceNotFoundException("named credential [" + name + "] not found"));
                } else {
                    listener.onResponse(List.of(credentialFromSource(name, source)));
                }
            }, listener::onFailure));
            return;
        }
        final SecurityIndexManager.IndexState indexState = indexManager.forCurrentProject();
        if (indexState.indexExists() == false) {
            listener.onResponse(List.of());
            return;
        }
        if (indexState.isAvailable(SecurityIndexManager.Availability.SEARCH_SHARDS) == false) {
            listener.onFailure(indexState.getUnavailableReason(SecurityIndexManager.Availability.SEARCH_SHARDS));
            return;
        }
        final SearchRequest searchRequest = client.prepareSearch(indexManager.aliasName())
            .setQuery(QueryBuilders.matchAllQuery())
            .setSize(MAX_CREDENTIALS)
            .setFetchSource(true)
            .addSort(SortBuilders.fieldSort("created_at").order(SortOrder.ASC))
            .request();
        indexState.checkIndexVersionThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                searchRequest,
                ActionListener.<SearchResponse>wrap(response -> {
                    List<NamedCredential> credentials = new ArrayList<>();
                    for (SearchHit hit : response.getHits().getHits()) {
                        credentials.add(credentialFromSource(hit.getId(), hit.getSourceAsMap()));
                    }
                    listener.onResponse(credentials);
                }, listener::onFailure),
                client::search
            )
        );
    }

    /**
     * Deletes the named credential. Responds {@code false} when not found.
     */
    public void deleteCredential(String name, ActionListener<Boolean> listener) {
        final SecurityIndexManager.IndexState indexState = indexManager.forCurrentProject();
        if (indexState.indexExists() == false) {
            listener.onResponse(false);
            return;
        }
        if (indexState.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS) == false) {
            listener.onFailure(indexState.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS));
            return;
        }
        final DeleteRequest deleteRequest = client.prepareDelete(indexManager.aliasName(), name)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .request();
        indexState.checkIndexVersionThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                deleteRequest,
                ActionListener.<DeleteResponse>wrap(
                    response -> listener.onResponse(response.getResult() == DocWriteResponse.Result.DELETED),
                    listener::onFailure
                ),
                client::delete
            )
        );
    }

    /**
     * Returns the named credential with the auth block decrypted and populated.
     */
    public void decryptCredential(String name, ActionListener<NamedCredential> listener) {
        final EncryptionService encryption = requireEncryptionService(listener);
        if (encryption == null) {
            return;
        }
        getRawSource(name, ActionListener.<Map<String, Object>>wrap(source -> {
            if (source == null) {
                listener.onFailure(new org.elasticsearch.ResourceNotFoundException("named credential [" + name + "] not found"));
                return;
            }
            final NamedCredential redacted = credentialFromSource(name, source);
            final String authBlob = (String) source.get("auth");
            if (authBlob == null) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "named credential [{}] has no stored auth block",
                        RestStatus.INTERNAL_SERVER_ERROR,
                        name
                    )
                );
                return;
            }
            final Map<String, String> auth = decryptAuth(encryption, authBlob);
            listener.onResponse(
                new NamedCredential(
                    redacted.name(),
                    redacted.authType(),
                    redacted.url(),
                    redacted.config(),
                    auth,
                    redacted.createdAtMillis(),
                    redacted.updatedAtMillis()
                )
            );
        }, listener::onFailure));
    }

    /**
     * Fetches the raw document source together with its sequence number metadata for optimistic concurrency,
     * or {@code null} if the document (or the index) does not exist.
     */
    private void getRawGetResult(String name, ActionListener<RawGetResult> listener) {
        final SecurityIndexManager.IndexState indexState = indexManager.forCurrentProject();
        if (indexState.indexExists() == false) {
            listener.onResponse(null);
            return;
        }
        if (indexState.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS) == false) {
            listener.onFailure(indexState.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS));
            return;
        }
        final GetRequest getRequest = client.prepareGet(indexManager.aliasName(), name).request();
        indexState.checkIndexVersionThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                getRequest,
                ActionListener.<GetResponse>wrap(
                    response -> listener.onResponse(
                        response.isExists() ? new RawGetResult(response.getSource(), response.getSeqNo(), response.getPrimaryTerm()) : null
                    ),
                    listener::onFailure
                ),
                client::get
            )
        );
    }

    /**
     * Fetches the raw document source, or {@code null} if the document (or the index) does not exist.
     */
    private void getRawSource(String name, ActionListener<Map<String, Object>> listener) {
        final SecurityIndexManager.IndexState indexState = indexManager.forCurrentProject();
        if (indexState.indexExists() == false) {
            listener.onResponse(null);
            return;
        }
        if (indexState.isAvailable(SecurityIndexManager.Availability.PRIMARY_SHARDS) == false) {
            listener.onFailure(indexState.getUnavailableReason(SecurityIndexManager.Availability.PRIMARY_SHARDS));
            return;
        }
        final GetRequest getRequest = client.prepareGet(indexManager.aliasName(), name).request();
        indexState.checkIndexVersionThenExecute(
            listener::onFailure,
            () -> executeAsyncWithOrigin(
                client.threadPool().getThreadContext(),
                SECURITY_ORIGIN,
                getRequest,
                ActionListener.<GetResponse>wrap(
                    response -> listener.onResponse(response.isExists() ? response.getSource() : null),
                    listener::onFailure
                ),
                client::get
            )
        );
    }

    private <T> EncryptionService requireEncryptionService(ActionListener<T> listener) {
        try {
            EncryptionService service = encryptionServiceSupplier.get();
            if (service == null) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        "the encryption service is not available on this cluster; named credentials cannot be used",
                        RestStatus.SERVICE_UNAVAILABLE
                    )
                );
                return null;
            }
            return service;
        } catch (IllegalStateException e) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "the encryption service is not available on this cluster; named credentials cannot be used",
                    RestStatus.SERVICE_UNAVAILABLE,
                    e
                )
            );
            return null;
        }
    }

    // Visible for testing

    static Map<String, Object> buildSource(
        PutNamedCredentialAction.Request request,
        Map<String, String> config,
        String authBlobBase64,
        long createdAt,
        long updatedAt
    ) {
        final Map<String, Object> source = new HashMap<>();
        source.put("auth_type", request.authType().typeName());
        if (request.url() != null) {
            source.put("url", request.url());
        }
        source.put("config", config);
        source.put("auth", authBlobBase64);
        source.put("created_at", createdAt);
        source.put("updated_at", updatedAt);
        return source;
    }

    @SuppressWarnings("unchecked")
    static NamedCredential credentialFromSource(String name, Map<String, Object> source) {
        final CredentialAuthType authType = CredentialAuthType.fromTypeName((String) source.get("auth_type"));
        final Map<String, String> config = source.get("config") == null ? Map.of() : Map.copyOf((Map<String, String>) source.get("config"));
        return new NamedCredential(
            name,
            authType,
            (String) source.get("url"),
            config,
            null,
            ((Number) source.get("created_at")).longValue(),
            ((Number) source.get("updated_at")).longValue()
        );
    }

    static byte[] serializeAuth(Map<String, String> auth) {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeMap(auth, StreamOutput::writeString, StreamOutput::writeString);
            return BytesReference.toBytes(out.bytes());
        } catch (IOException e) {
            throw new ElasticsearchStatusException("failed to serialize credential auth block", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
    }

    static String encryptAuth(EncryptionService encryption, Map<String, String> auth) {
        final byte[] plaintext = serializeAuth(auth);
        try {
            final EncryptedData encrypted = encryption.encrypt(plaintext);
            try (var builder = XContentFactory.jsonBuilder()) {
                encrypted.toXContent(builder, ToXContent.EMPTY_PARAMS);
                return Base64.getEncoder().encodeToString(BytesReference.toBytes(BytesReference.bytes(builder)));
            } catch (IOException e) {
                throw new ElasticsearchStatusException("failed to serialize encrypted auth block", RestStatus.INTERNAL_SERVER_ERROR, e);
            }
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    static Map<String, String> decryptAuth(EncryptionService encryption, String base64Blob) {
        final byte[] blobBytes = Base64.getDecoder().decode(base64Blob);
        final EncryptedData encrypted;
        try (
            var parser = XContentType.JSON.xContent()
                .createParser(XContentParserConfiguration.EMPTY, new BytesArray(blobBytes).streamInput())
        ) {
            encrypted = EncryptedData.fromXContent(parser);
        } catch (IOException | XContentParseException e) {
            throw new ElasticsearchStatusException("stored auth block is malformed", RestStatus.INTERNAL_SERVER_ERROR, e);
        }
        final byte[] plaintext = encryption.decrypt(encrypted);
        try (StreamInput in = new BytesArray(plaintext).streamInput()) {
            return in.readMap(StreamInput::readString, StreamInput::readString);
        } catch (IOException e) {
            throw new ElasticsearchStatusException("decrypted auth block is malformed", RestStatus.INTERNAL_SERVER_ERROR, e);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }
}
