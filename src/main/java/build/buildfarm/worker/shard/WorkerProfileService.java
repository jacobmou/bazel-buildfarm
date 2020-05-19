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
import build.buildfarm.v1test.WorkerProfileGrpc;
import build.buildfarm.v1test.WorkerProfileMessage;
import build.buildfarm.v1test.WorkerProfileRequest;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.WorkerContext;
import io.grpc.stub.StreamObserver;
import java.util.logging.Logger;

public class WorkerProfileService extends WorkerProfileGrpc.WorkerProfileImplBase {
  private static final Logger logger = Logger.getLogger(WorkerProfileService.class.getName());

  private final CASFileCache storage;
  private final InputFetchStage inputFetchStage;
  private final ExecuteActionStage executeActionStage;
  private final WorkerContext context;

  public WorkerProfileService(
      ContentAddressableStorage storage,
      PipelineStage inputFetchStage,
      PipelineStage executeActionStage,
      WorkerContext context) {
    this.storage = (CASFileCache) storage;
    this.inputFetchStage = (InputFetchStage) inputFetchStage;
    this.executeActionStage = (ExecuteActionStage) executeActionStage;
    this.context = context;
  }

  @Override
  public void getWorkerProfile(
      WorkerProfileRequest request, StreamObserver<WorkerProfileMessage> responseObserver) {

    WorkerProfileMessage.Builder replyBuilder =
        WorkerProfileMessage.newBuilder()
            .setCASEntryCount(storage.storageCount())
            .setCASDirectoryEntryCount(storage.directoryStorageCount())
            .setEntryContainingDirectoriesCount(CASFileCache.Entry.containedDirectoriesCount)
            .setEntryContainingDirectoriesMax(CASFileCache.Entry.containedDirectoriesMax)
            .setCASEvictedEntryCount(storage.getEvictedCount())
            .setCASEvictedEntrySize(storage.getEvictedSize());

    // get slots usage/configure of stages
    String inputFetchStageSlotUsage =
        String.format("%d/%d", inputFetchStage.getSlotUsage(), context.getInputFetchStageWidth());
    String executeActionStageSlotUsage =
        String.format("%d/%d", executeActionStage.getSlotUsage(), context.getExecuteStageWidth());

    // get Opeartion

    replyBuilder
        .setInputFetchStageSlotsUsedOverConfigured(inputFetchStageSlotUsage)
        .setExecuteActionStageSlotsUsedOverConfigured(executeActionStageSlotUsage);

    responseObserver.onNext(replyBuilder.build());
    responseObserver.onCompleted();
  }
}