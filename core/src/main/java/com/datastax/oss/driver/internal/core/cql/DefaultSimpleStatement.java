/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultSimpleStatement implements SimpleStatement {

  private final String query;
  private final List<Object> positionalValues;
  private final Map<CqlIdentifier, Object> namedValues;
  private final String executionProfileName;
  private final DriverExecutionProfile executionProfile;
  private final CqlIdentifier keyspace;
  private final CqlIdentifier routingKeyspace;
  private final ByteBuffer routingKey;
  private final Token routingToken;

  private final Map<String, ByteBuffer> customPayload;
  private final Boolean idempotent;
  private final boolean tracing;
  private final long timestamp;
  private final ByteBuffer pagingState;
  private final int pageSize;
  private final ConsistencyLevel consistencyLevel;
  private final ConsistencyLevel serialConsistencyLevel;
  private final Duration timeout;

  /** @see SimpleStatement#builder(String) */
  public DefaultSimpleStatement(
      String query,
      List<Object> positionalValues,
      Map<CqlIdentifier, Object> namedValues,
      String executionProfileName,
      DriverExecutionProfile executionProfile,
      CqlIdentifier keyspace,
      CqlIdentifier routingKeyspace,
      ByteBuffer routingKey,
      Token routingToken,
      Map<String, ByteBuffer> customPayload,
      Boolean idempotent,
      boolean tracing,
      long timestamp,
      ByteBuffer pagingState,
      int pageSize,
      ConsistencyLevel consistencyLevel,
      ConsistencyLevel serialConsistencyLevel,
      Duration timeout) {
    if (!positionalValues.isEmpty() && !namedValues.isEmpty()) {
      throw new IllegalArgumentException("Can't have both positional and named values");
    }
    this.query = query;
    this.positionalValues = NullAllowingImmutableList.copyOf(positionalValues);
    this.namedValues = NullAllowingImmutableMap.copyOf(namedValues);
    this.executionProfileName = executionProfileName;
    this.executionProfile = executionProfile;
    this.keyspace = keyspace;
    this.routingKeyspace = routingKeyspace;
    this.routingKey = routingKey;
    this.routingToken = routingToken;
    this.customPayload = customPayload;
    this.idempotent = idempotent;
    this.tracing = tracing;
    this.timestamp = timestamp;
    this.pagingState = pagingState;
    this.pageSize = pageSize;
    this.consistencyLevel = consistencyLevel;
    this.serialConsistencyLevel = serialConsistencyLevel;
    this.timeout = timeout;
  }

  @NonNull
  @Override
  public String getQuery() {
    return query;
  }

  @NonNull
  @Override
  public SimpleStatement setQuery(@NonNull String newQuery) {
    return new DefaultSimpleStatement(
        newQuery,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @NonNull
  @Override
  public List<Object> getPositionalValues() {
    return positionalValues;
  }

  @NonNull
  @Override
  public SimpleStatement setPositionalValues(@NonNull List<Object> newPositionalValues) {
    return new DefaultSimpleStatement(
        query,
        newPositionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, Object> getNamedValues() {
    return namedValues;
  }

  @NonNull
  @Override
  public SimpleStatement setNamedValuesWithIds(@NonNull Map<CqlIdentifier, Object> newNamedValues) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        newNamedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Nullable
  @Override
  public String getExecutionProfileName() {
    return executionProfileName;
  }

  @NonNull
  @Override
  public SimpleStatement setExecutionProfileName(@Nullable String newConfigProfileName) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        newConfigProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Nullable
  @Override
  public DriverExecutionProfile getExecutionProfile() {
    return executionProfile;
  }

  @NonNull
  @Override
  public SimpleStatement setExecutionProfile(@Nullable DriverExecutionProfile newProfile) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        null,
        newProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Nullable
  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  @Override
  public SimpleStatement setKeyspace(@Nullable CqlIdentifier newKeyspace) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        newKeyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Nullable
  @Override
  public CqlIdentifier getRoutingKeyspace() {
    return routingKeyspace;
  }

  @NonNull
  @Override
  public SimpleStatement setRoutingKeyspace(@Nullable CqlIdentifier newRoutingKeyspace) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        newRoutingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Nullable
  @Override
  public ByteBuffer getRoutingKey() {
    return routingKey;
  }

  @NonNull
  @Override
  public SimpleStatement setRoutingKey(@Nullable ByteBuffer newRoutingKey) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        newRoutingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Nullable
  @Override
  public Token getRoutingToken() {
    return routingToken;
  }

  @NonNull
  @Override
  public SimpleStatement setRoutingToken(@Nullable Token newRoutingToken) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        newRoutingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @NonNull
  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }

  @NonNull
  @Override
  public SimpleStatement setCustomPayload(@NonNull Map<String, ByteBuffer> newCustomPayload) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        newCustomPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Nullable
  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  @NonNull
  @Override
  public SimpleStatement setIdempotent(@Nullable Boolean newIdempotence) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        newIdempotence,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @NonNull
  @Override
  public SimpleStatement setTracing(boolean newTracing) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        newTracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @NonNull
  @Override
  public SimpleStatement setTimestamp(long newTimestamp) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        newTimestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Nullable
  @Override
  public Duration getTimeout() {
    return timeout;
  }

  @NonNull
  @Override
  public SimpleStatement setTimeout(@Nullable Duration newTimeout) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        newTimeout);
  }

  @Nullable
  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @NonNull
  @Override
  public SimpleStatement setPagingState(@Nullable ByteBuffer newPagingState) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        newPagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Override
  public int getPageSize() {
    return pageSize;
  }

  @NonNull
  @Override
  public SimpleStatement setPageSize(int newPageSize) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        newPageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Nullable
  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  @Override
  public SimpleStatement setConsistencyLevel(@Nullable ConsistencyLevel newConsistencyLevel) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        newConsistencyLevel,
        serialConsistencyLevel,
        timeout);
  }

  @Nullable
  @Override
  public ConsistencyLevel getSerialConsistencyLevel() {
    return serialConsistencyLevel;
  }

  @NonNull
  @Override
  public SimpleStatement setSerialConsistencyLevel(
      @Nullable ConsistencyLevel newSerialConsistencyLevel) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        newSerialConsistencyLevel,
        timeout);
  }

  public static Map<CqlIdentifier, Object> wrapKeys(Map<String, Object> namedValues) {
    NullAllowingImmutableMap.Builder<CqlIdentifier, Object> builder =
        NullAllowingImmutableMap.builder();
    for (Map.Entry<String, Object> entry : namedValues.entrySet()) {
      builder.put(CqlIdentifier.fromCql(entry.getKey()), entry.getValue());
    }
    return builder.build();
  }
}