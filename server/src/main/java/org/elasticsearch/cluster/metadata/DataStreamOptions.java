/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * Holds data stream dedicated configuration options such as failure store, (in the future lifecycle). Currently, it
 * supports the following configurations:
 * - lifecycle
 * - failure store
 */
public record DataStreamOptions(@Nullable DataStreamLifecycle lifecycle, @Nullable DataStreamFailureStore failureStore)
    implements
        SimpleDiffable<DataStreamOptions>,
        ToXContentObject {

    public static final ParseField LIFECYCLE_FIELD = new ParseField("lifecycle");
    public static final ParseField FAILURE_STORE_FIELD = new ParseField("failure_store");

    public static final ConstructingObjectParser<DataStreamOptions, Void> PARSER = new ConstructingObjectParser<>(
        "options",
        false,
        (args, unused) -> new DataStreamOptions((DataStreamLifecycle) args[0], (DataStreamFailureStore) args[1])
    );

    static {
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DataStreamLifecycle.fromXContent(p),
            LIFECYCLE_FIELD,
            ObjectParser.ValueType.OBJECT_OR_NULL
        );
        PARSER.declareField(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> DataStreamFailureStore.fromXContent(p),
            FAILURE_STORE_FIELD,
            ObjectParser.ValueType.OBJECT_OR_NULL
        );
    }

    public DataStreamOptions() {
        this(null, null);
    }

    public static DataStreamOptions read(StreamInput in) throws IOException {
        return new DataStreamOptions(
            in.readOptionalWriteable(DataStreamLifecycle::new),
            in.readOptionalWriteable(DataStreamFailureStore::read)
        );
    }

    public static Diff<DataStreamOptions> readDiffFrom(StreamInput in) throws IOException {
        return SimpleDiffable.readDiffFrom(DataStreamOptions::read, in);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(DataStreamOptions dataStreamOptions) {
        return new Builder(dataStreamOptions.lifecycle, dataStreamOptions.failureStore);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(lifecycle);
        out.writeOptionalWriteable(failureStore);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params, RolloverConfiguration rolloverConfiguration)
        throws IOException {
        builder.startObject();
        if (lifecycle != null) {
            builder.field(LIFECYCLE_FIELD.getPreferredName());
            lifecycle.toXContent(builder, params, rolloverConfiguration, null);
        }
        if (failureStore != null) {
            builder.field(FAILURE_STORE_FIELD.getPreferredName(), failureStore);
        }
        builder.endObject();
        return builder;
    }

    public static DataStreamOptions fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public static class Builder {
        @Nullable
        private DataStreamLifecycle.Builder lifecycle;
        @Nullable
        private DataStreamFailureStore.Builder failureStore;

        public Builder() {}

        public Builder(DataStreamLifecycle lifecycle, DataStreamFailureStore failureStore) {
            if (lifecycle != null) {
                this.lifecycle = DataStreamLifecycle.newBuilder(lifecycle);
            }
            if (failureStore != null) {
                this.failureStore = DataStreamFailureStore.newBuilder(failureStore);
            }
        }

        public Builder override(DataStreamOptions dataStreamOptions) {
            if (dataStreamOptions.lifecycle != null) {
                if (lifecycle == null) {
                    lifecycle = DataStreamLifecycle.newBuilder(dataStreamOptions.lifecycle);
                } else {
                    lifecycle.override(dataStreamOptions.lifecycle);
                }
            }
            if (dataStreamOptions.failureStore != null) {
                if (failureStore == null) {
                    failureStore = DataStreamFailureStore.newBuilder(dataStreamOptions.failureStore);
                } else {
                    failureStore.override(dataStreamOptions.failureStore);
                }
            }
            return this;
        }

        public Builder overrideLifecycle(DataStreamLifecycle lifecycle) {
            if (this.lifecycle == null) {
                this.lifecycle = DataStreamLifecycle.newBuilder(lifecycle);
            } else {
                this.lifecycle.override(lifecycle);
            }
            return this;
        }

        public Builder setLifecycle(DataStreamLifecycle lifecycle) {
            if (lifecycle == null) {
                this.lifecycle = null;
            } else {
                this.lifecycle = DataStreamLifecycle.newBuilder(lifecycle);
            }
            return this;
        }

        public Builder setFailureStore(DataStreamFailureStore failureStore) {
            if (failureStore == null) {
                this.failureStore = null;
            } else {
                this.failureStore = DataStreamFailureStore.newBuilder(failureStore);
            }
            return this;
        }

        public DataStreamOptions build() {
            return new DataStreamOptions(lifecycle == null ? null : lifecycle.build(), failureStore == null ? null : failureStore.build());
        }
    }
}
