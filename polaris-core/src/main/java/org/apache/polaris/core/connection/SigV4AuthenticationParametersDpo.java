/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.core.connection;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Map;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.polaris.core.admin.model.AuthenticationParameters;
import org.apache.polaris.core.admin.model.SigV4AuthenticationParameters;
import org.apache.polaris.core.secrets.UserSecretsManager;

/**
 * The internal persistence-object counterpart to SigV4AuthenticationParameters defined in the API
 * model.
 */
public class SigV4AuthenticationParametersDpo extends AuthenticationParametersDpo {

  @JsonProperty(value = "roleArn")
  private final String roleArn;

  @JsonProperty(value = "externalId")
  private final String externalId;

  @JsonProperty(value = "signingRegion")
  private final String signingRegion;

  @JsonProperty(value = "signingName")
  private final String signingName;

  @JsonProperty(value = "userArn")
  private final String userArn;

  public SigV4AuthenticationParametersDpo(
      @JsonProperty(value = "roleArn", required = true) String roleArn,
      @JsonProperty(value = "externalId", required = false) String externalId,
      @JsonProperty(value = "signingRegion", required = false) String signingRegion,
      @JsonProperty(value = "signingName", required = false) String signingName,
      @JsonProperty(value = "userArn", required = false) String userArn) {
    super(AuthenticationType.SIGV4.getCode());
    this.roleArn = roleArn;
    this.externalId = externalId;
    this.signingRegion = signingRegion;
    this.signingName = signingName;
    this.userArn = userArn;
  }

  public @Nonnull String getRoleArn() {
    return roleArn;
  }

  public @Nullable String getExternalId() {
    return externalId;
  }

  public @Nullable String getSigningRegion() {
    return signingRegion;
  }

  public @Nullable String getSigningName() {
    return signingName;
  }

  public @Nullable String getUserArn() {
    return userArn;
  }

  @Nonnull
  @Override
  public Map<String, String> asIcebergCatalogProperties(UserSecretsManager secretsManager) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.put(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_SIGV4);
    if (getSigningRegion() != null) {
      builder.put(AwsProperties.REST_SIGNER_REGION, getSigningRegion());
    }
    if (getSigningName() != null) {
      builder.put(AwsProperties.REST_SIGNING_NAME, getSigningName());
    }
    // TODO: Add a connection credential provider to get the tmp aws credentials for SigV4 auth
    builder.put(AwsProperties.REST_ACCESS_KEY_ID, "access_key_id");
    builder.put(AwsProperties.REST_SECRET_ACCESS_KEY, "secret_access_key");
    builder.put(AwsProperties.REST_SESSION_TOKEN, "session_token");
    return builder.build();
  }

  @Override
  public @Nonnull AuthenticationParameters asAuthenticationParametersModel() {
    return SigV4AuthenticationParameters.builder()
        .setAuthenticationType(AuthenticationParameters.AuthenticationTypeEnum.SIGV4)
        .setRoleArn(this.roleArn)
        .setExternalId(this.externalId)
        .setSigningRegion(this.signingRegion)
        .setSigningName(this.signingName)
        .setUserArn(this.userArn)
        .build();
  }
}
