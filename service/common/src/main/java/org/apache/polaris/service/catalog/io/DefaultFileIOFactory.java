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
package org.apache.polaris.service.catalog.io;

import com.google.common.annotations.VisibleForTesting;
import io.smallrye.common.annotation.Identifier;
import jakarta.annotation.Nonnull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.context.RealmId;
import org.apache.polaris.core.entity.PolarisEntity;
import org.apache.polaris.core.persistence.MetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisEntityManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.core.persistence.PolarisResolvedPathWrapper;
import org.apache.polaris.core.storage.PolarisCredentialVendor;
import org.apache.polaris.core.storage.PolarisStorageActions;
import org.apache.polaris.service.config.RealmEntityManagerFactory;

/**
 * A default FileIO factory implementation for creating Iceberg {@link FileIO} instances with
 * contextual table-level properties.
 *
 * <p>This class acts as a translation layer between Polaris properties and the properties required
 * by Iceberg's {@link FileIO}. For example, it evaluates storage actions and retrieves subscoped
 * credentials to initialize a {@link FileIO} instance with the most limited permissions necessary.
 */
@ApplicationScoped
@Identifier("default")
public class DefaultFileIOFactory implements FileIOFactory {

  private final RealmEntityManagerFactory realmEntityManagerFactory;
  private final MetaStoreManagerFactory metaStoreManagerFactory;
  private final PolarisConfigurationStore configurationStore;

  @Inject
  public DefaultFileIOFactory(
      RealmEntityManagerFactory realmEntityManagerFactory,
      MetaStoreManagerFactory metaStoreManagerFactory,
      PolarisConfigurationStore configurationStore) {
    this.realmEntityManagerFactory = realmEntityManagerFactory;
    this.metaStoreManagerFactory = metaStoreManagerFactory;
    this.configurationStore = configurationStore;
  }

  @Override
  public FileIO loadFileIO(
      @Nonnull RealmId realmId,
      @Nonnull String ioImplClassName,
      @Nonnull Map<String, String> properties) {
    return loadFileIOInternal(ioImplClassName, properties);
  }

  @Override
  public FileIO loadFileIO(
      @Nonnull RealmId realmId,
      @Nonnull String ioImplClassName,
      @Nonnull Map<String, String> properties,
      @Nonnull TableIdentifier identifier,
      @Nonnull Set<String> tableLocations,
      @Nonnull Set<PolarisStorageActions> storageActions,
      @Nonnull PolarisResolvedPathWrapper resolvedEntityPath) {
    PolarisEntityManager entityManager =
        realmEntityManagerFactory.getOrCreateEntityManager(realmId);
    PolarisCredentialVendor credentialVendor =
        metaStoreManagerFactory.getOrCreateMetaStoreManager(realmId);
    PolarisMetaStoreSession metaStoreSession =
        metaStoreManagerFactory.getOrCreateSessionSupplier(realmId).get();

    // Get subcoped creds
    properties = new HashMap<>(properties);
    Optional<PolarisEntity> storageInfoEntity =
        FileIOUtil.findStorageInfoFromHierarchy(resolvedEntityPath);
    Map<String, String> credentialsMap =
        storageInfoEntity
            .map(
                storageInfo ->
                    FileIOUtil.refreshCredentials(
                        realmId,
                        entityManager,
                        credentialVendor,
                        metaStoreSession,
                        configurationStore,
                        identifier,
                        tableLocations,
                        storageActions,
                        storageInfo))
            .orElse(Map.of());

    // Update the FileIO before we write the new metadata file
    // update with properties in case there are table-level overrides the credentials should
    // always override table-level properties, since storage configuration will be found at
    // whatever entity defines it
    properties.putAll(credentialsMap);

    return loadFileIOInternal(ioImplClassName, properties);
  }

  @VisibleForTesting
  FileIO loadFileIOInternal(
      @Nonnull String ioImplClassName, @Nonnull Map<String, String> properties) {
    return CatalogUtil.loadFileIO(ioImplClassName, properties, new Configuration());
  }
}
