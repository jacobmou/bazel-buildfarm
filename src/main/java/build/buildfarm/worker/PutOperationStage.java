// Copyright 2018 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker;

import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import build.buildfarm.common.function.InterruptingConsumer;
import com.google.longrunning.Operation;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Durations;
import com.google.protobuf.util.Timestamps;
import java.util.Arrays;

public class PutOperationStage extends PipelineStage.NullStage {
  private final InterruptingConsumer<Operation> onPut;

  private volatile AverageTimeCostOfLastPeriod[] averagesWithinDifferentPeriods;

  public PutOperationStage(InterruptingConsumer<Operation> onPut) {
    this.onPut = onPut;
    this.averagesWithinDifferentPeriods =
        new AverageTimeCostOfLastPeriod[] {
          new AverageTimeCostOfLastPeriod(60),
          new AverageTimeCostOfLastPeriod(10 * 60),
          new AverageTimeCostOfLastPeriod(60 * 60),
          new AverageTimeCostOfLastPeriod(3 * 60 * 60),
          new AverageTimeCostOfLastPeriod(24 * 60 * 60)
        };
  }

  @Override
  public void put(OperationContext operationContext) throws InterruptedException {
    onPut.acceptInterruptibly(operationContext.operation);
    synchronized (this) {
      for (AverageTimeCostOfLastPeriod average : averagesWithinDifferentPeriods) {
        average.addOperation(
            operationContext.executeResponse.build().getResult().getExecutionMetadata());
      }
    }
  }

  public synchronized OperationStageDurations[] getAverageTimeCostPerStage() {
    return Arrays.stream(averagesWithinDifferentPeriods)
        .map(AverageTimeCostOfLastPeriod::getAverageOfLastPeriod)
        .toArray(OperationStageDurations[]::new);
  }

  private static class AverageTimeCostOfLastPeriod {
    static final int NumOfSlots = 100;
    private OperationStageDurations[] slots;
    private int lastUsedSlot = -1;
    private Duration period;
    private OperationStageDurations nextOperation;
    private OperationStageDurations averageTimeCosts;
    private Timestamp lastOperationCompleteTime;

    AverageTimeCostOfLastPeriod(int period) {
      this.period = Durations.fromSeconds(period);
      slots = new OperationStageDurations[NumOfSlots];
      for (int i = 0; i < slots.length; i++) {
        slots[i] = new OperationStageDurations();
      }
      nextOperation = new OperationStageDurations();
      averageTimeCosts = new OperationStageDurations();
    }

    private void removeStaleData(Timestamp now) {
      // currentSlot != lastUsedSlot means stepping over to a new slot.
      // The data in the new slot should be thrown away before storing new data.
      int currentSlot = getCurrentSlot(now);
      if (lastOperationCompleteTime != null && lastUsedSlot >= 0) {
        Duration duration = Timestamps.between(lastOperationCompleteTime, now);
        // if 1) duration between the new added operation and last added one is longer than period
        // or 2) the duration is shorter than period but longer than time range of a single slot
        //       and at the same time currentSlot == lastUsedSlot
        if ((Durations.toMillis(duration) >= Durations.toMillis(period))
            || (lastUsedSlot == currentSlot
                && Durations.toMillis(duration) > (Durations.toMillis(period) / slots.length))) {
          for (OperationStageDurations slot : slots) {
            slot.reset();
          }
        } else if (lastUsedSlot != currentSlot) {
          // currentSlot < lastUsedSlot means wrap around happened
          currentSlot = currentSlot < lastUsedSlot ? currentSlot + NumOfSlots : currentSlot;
          for (int i = lastUsedSlot + 1; i <= currentSlot; i++) {
            slots[i % slots.length].reset();
          }
        }
      }
    }

    // Compute the slot index the Timestamp would belong to
    private int getCurrentSlot(Timestamp time) {
      long millisPeriod = Durations.toMillis(period);
      return (int) (Timestamps.toMillis(time) % millisPeriod / (millisPeriod / slots.length));
    }

    OperationStageDurations getAverageOfLastPeriod() {
      // creating a Timestamp representing now to trigger stale data throwing away
      Timestamp now = Timestamps.fromMillis(System.currentTimeMillis());
      removeStaleData(now);
      averageTimeCosts.reset();
      for (OperationStageDurations slot : slots) {
        averageTimeCosts.addOperations(slot);
      }
      averageTimeCosts.period = period;
      return averageTimeCosts;
    }

    void addOperation(ExecutedActionMetadata metadata) {
      // remove stale data first
      Timestamp completeTime = metadata.getOutputUploadCompletedTimestamp();
      removeStaleData(completeTime);

      // add new ExecutedOperation metadata
      int currentSlot = getCurrentSlot(completeTime);
      nextOperation.set(metadata);
      slots[currentSlot].addOperations(nextOperation);

      lastOperationCompleteTime = completeTime;
      lastUsedSlot = currentSlot;
    }
  }

  // when operationCount == 1, an object represents one operation's time costs on each stage;
  // when operationCount > 1, an object represents aggregated time costs of multiple operations.
  public static class OperationStageDurations {
    public Duration queuedToMatch;
    public Duration matchToInputFetchStart;
    public Duration inputFetchStartToComplete;
    public Duration inputFetchCompleteToExecutionStart;
    public Duration executionStartToComplete;
    public Duration executionCompleteToOutputUploadStart;
    public Duration outputUploadStartToComplete;
    public Duration period;
    public int operationCount;

    public OperationStageDurations() {
      reset();
    }

    void set(ExecutedActionMetadata metadata) {
      queuedToMatch =
          Timestamps.between(metadata.getQueuedTimestamp(), metadata.getWorkerStartTimestamp());
      matchToInputFetchStart =
          Timestamps.between(
              metadata.getWorkerStartTimestamp(), metadata.getInputFetchStartTimestamp());
      inputFetchStartToComplete =
          Timestamps.between(
              metadata.getInputFetchStartTimestamp(), metadata.getInputFetchCompletedTimestamp());
      inputFetchCompleteToExecutionStart =
          Timestamps.between(
              metadata.getInputFetchCompletedTimestamp(), metadata.getExecutionStartTimestamp());
      executionStartToComplete =
          Timestamps.between(
              metadata.getExecutionStartTimestamp(), metadata.getExecutionCompletedTimestamp());
      executionCompleteToOutputUploadStart =
          Timestamps.between(
              metadata.getExecutionCompletedTimestamp(), metadata.getOutputUploadStartTimestamp());
      outputUploadStartToComplete =
          Timestamps.between(
              metadata.getOutputUploadStartTimestamp(),
              metadata.getOutputUploadCompletedTimestamp());
      operationCount = 1;
    }

    void reset() {
      queuedToMatch = Duration.newBuilder().build();
      matchToInputFetchStart = Duration.newBuilder().build();
      inputFetchStartToComplete = Duration.newBuilder().build();
      inputFetchCompleteToExecutionStart = Duration.newBuilder().build();
      executionStartToComplete = Duration.newBuilder().build();
      executionCompleteToOutputUploadStart = Duration.newBuilder().build();
      outputUploadStartToComplete = Duration.newBuilder().build();
      operationCount = 0;
    }

    void addOperations(OperationStageDurations other) {
      this.queuedToMatch =
          computeAverage(
              this.queuedToMatch,
              this.operationCount,
              other.queuedToMatch,
              other.operationCount);
      this.matchToInputFetchStart =
          computeAverage(
              this.matchToInputFetchStart,
              this.operationCount,
              other.matchToInputFetchStart,
              other.operationCount);
      this.inputFetchStartToComplete =
          computeAverage(
              this.inputFetchStartToComplete,
              this.operationCount,
              other.inputFetchStartToComplete,
              other.operationCount);
      this.inputFetchCompleteToExecutionStart =
          computeAverage(
              this.inputFetchCompleteToExecutionStart,
              this.operationCount,
              other.inputFetchCompleteToExecutionStart,
              other.operationCount);
      this.executionStartToComplete =
          computeAverage(
              this.executionStartToComplete,
              this.operationCount,
              other.executionStartToComplete,
              other.operationCount);
      this.executionCompleteToOutputUploadStart =
          computeAverage(
              this.executionCompleteToOutputUploadStart,
              this.operationCount,
              other.executionCompleteToOutputUploadStart,
              other.operationCount);
      this.outputUploadStartToComplete =
          computeAverage(
              this.outputUploadStartToComplete,
              this.operationCount,
              other.outputUploadStartToComplete,
              other.operationCount);
      this.operationCount += other.operationCount;
    }

    private static Duration computeAverage(
        Duration d1, int operationCount1, Duration d2, int operationCount2) {
      if (operationCount1 == 0 && operationCount2 == 0) {
        return Durations.fromNanos(0);
      }
      long averageNanos =
          (Durations.toNanos(d1) * operationCount1 + Durations.toNanos(d2) * operationCount2)
              / (operationCount1 + operationCount2);
      return Durations.fromNanos(averageNanos);
    }

    private static float millisecondBetween(Timestamp from, Timestamp to) {
      // The time unit we want is millisecond.
      // 1 second = 1000 milliseconds
      // 1 millisecond = 1000,000 nanoseconds
      Duration d = Timestamps.between(from, to);
      return Durations.toNanos(d) / (1000.0f * 1000.0f);
    }
  }
}
