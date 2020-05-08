// Copyright 2020 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.worker.shard;

import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.v1test.CASUsageProfileGrpc;
import build.buildfarm.v1test.CASUsageMessage;
import build.buildfarm.v1test.CASUsageRequest;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.worker.CASFileCache.Entry;
import io.grpc.stub.StreamObserver;
import java.util.logging.Logger;

public class CASMemoryProfileService extends CASUsageProfileGrpc.CASUsageProfileImplBase{
  private static final Logger logger = Logger.getLogger(CASMemoryProfileService.class.getName());

  private final CASFileCache storage;

  public CASMemoryProfileService(ContentAddressableStorage storage) {
    this.storage = (CASFileCache) storage;
  }

  @Override
  public void getCASUsage(
      CASUsageRequest request, StreamObserver<CASUsageMessage> responseObserver) {

    long [] containedDirectories = storage.getContainedDirectoriesCounts();

    CASUsageMessage reply =
        CASUsageMessage.newBuilder()
            .setEntryCount(storage.storageCount())
            .setDirectoryEntryCount(storage.directoryStorageCount())
            .setContainingDirectoriesCount(containedDirectories[0])
            .setContainingDirectoriesMax(containedDirectories[1])
            .setEvictedEntryCount(storage.getEvictedCount())
            .setEvictedEntrySize(storage.getEvictedSize())
            .build();

    responseObserver.onNext(reply);
    responseObserver.onCompleted();
  }
}
