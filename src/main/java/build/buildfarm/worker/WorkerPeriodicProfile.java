package build.buildfarm.worker;

import build.bazel.remote.execution.v2.ExecutedActionMetadata;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;

import java.util.Arrays;

public class WorkerPeriodicProfile {
  private final AverageOperationTimeCostPerPeriod[] averages;
  private final TotalEvictedEntryPerPeriod[] totals;

  private static WorkerPeriodicProfile singleInstance;

  public static WorkerPeriodicProfile create() {
    if (singleInstance == null) {
      singleInstance = new WorkerPeriodicProfile();
    }
    return singleInstance;
  }

  private WorkerPeriodicProfile () {
    this.averages =
        new AverageOperationTimeCostPerPeriod[] {
            new AverageOperationTimeCostPerPeriod(100),
            new AverageOperationTimeCostPerPeriod(10 * 60),
            new AverageOperationTimeCostPerPeriod(60 * 60),
            new AverageOperationTimeCostPerPeriod(3 * 60 * 60),
            new AverageOperationTimeCostPerPeriod(24 * 60 * 60)
        };
    this.totals =
        new TotalEvictedEntryPerPeriod[] {
            new TotalEvictedEntryPerPeriod()
        };
  }

  public void addOperation(ExecutedActionMetadata executionMetadata) {
    for (AverageOperationTimeCostPerPeriod average : averages) {
      average.addOperation(executionMetadata);
    }
  }

  public OperationTimeCostOnStages[] getAverageTimeCostPerStage() {
    return Arrays.stream(averages)
        .map(AverageOperationTimeCostPerPeriod::getAverageOfLastPeriod)
        .toArray(OperationTimeCostOnStages[]::new);
  }

  static class TotalEvictedEntryPerPeriod {

  }

  static class AverageOperationTimeCostPerPeriod {
    static final int NumOfSlots = 100;
    private OperationTimeCostOnStages[] slots;
    private int lastUsedSlot = -1;
    private int period;
    private OperationTimeCostOnStages nextOperation;
    private OperationTimeCostOnStages averageTimeCosts;
    private Timestamp lastOperationCompleteTime;

    AverageOperationTimeCostPerPeriod(int period) {
      this.period = period;
      slots = new OperationTimeCostOnStages[NumOfSlots];
      for (int i = 0; i < slots.length; i++) {
        slots[i] = new OperationTimeCostOnStages();
      }
      nextOperation = new OperationTimeCostOnStages();
      averageTimeCosts = new OperationTimeCostOnStages();
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
        if ((duration.getSeconds() >= this.period)
            || (lastUsedSlot == currentSlot
                && duration.getSeconds() > (this.period / slots.length))) {
          for (OperationTimeCostOnStages slot : slots) {
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

    private int getCurrentSlot(Timestamp time) {
      return (int) time.getSeconds() % period / (period / slots.length);
    }

    OperationTimeCostOnStages getAverageOfLastPeriod() {
      // creating a Timestamp representing now to trigger stale data throwing away
      Timestamp now = Timestamps.fromMillis(System.currentTimeMillis());
      removeStaleData(now);
      averageTimeCosts.reset();
      for (OperationTimeCostOnStages slot : slots) {
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
  public static class OperationTimeCostOnStages {
    public float queuedToMatch;
    public float matchToInputFetchStart;
    public float inputFetchStartToComplete;
    public float inputFetchCompleteToExecutionStart;
    public float executionStartToComplete;
    public float executionCompleteToOutputUploadStart;
    public float outputUploadStartToComplete;
    public int operationCount;
    public int period;

    void set(ExecutedActionMetadata metadata) {
      queuedToMatch =
          millisecondBetween(metadata.getQueuedTimestamp(), metadata.getWorkerStartTimestamp());
      matchToInputFetchStart =
          millisecondBetween(
              metadata.getWorkerStartTimestamp(), metadata.getInputFetchStartTimestamp());
      inputFetchStartToComplete =
          millisecondBetween(
              metadata.getInputFetchStartTimestamp(), metadata.getInputFetchCompletedTimestamp());
      inputFetchCompleteToExecutionStart =
          millisecondBetween(
              metadata.getInputFetchCompletedTimestamp(), metadata.getExecutionStartTimestamp());
      executionStartToComplete =
          millisecondBetween(
              metadata.getExecutionStartTimestamp(), metadata.getExecutionCompletedTimestamp());
      executionCompleteToOutputUploadStart =
          millisecondBetween(
              metadata.getExecutionCompletedTimestamp(), metadata.getOutputUploadStartTimestamp());
      outputUploadStartToComplete =
          millisecondBetween(
              metadata.getOutputUploadStartTimestamp(),
              metadata.getOutputUploadCompletedTimestamp());
      operationCount = 1;
    }

    void reset() {
      queuedToMatch = 0.0f;
      matchToInputFetchStart = 0.0f;
      inputFetchStartToComplete = 0.0f;
      inputFetchCompleteToExecutionStart = 0.0f;
      executionStartToComplete = 0.0f;
      executionCompleteToOutputUploadStart = 0.0f;
      outputUploadStartToComplete = 0.0f;
      operationCount = 0;
    }

    void addOperations(OperationTimeCostOnStages other) {
      this.queuedToMatch =
          computeAverage(
              this.queuedToMatch, this.operationCount, other.queuedToMatch, other.operationCount);
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

    private static float computeAverage(
        float time1, int operationCount1, float time2, int operationCount2) {
      if (operationCount1 == 0 && operationCount2 == 0) {
        return 0.0f;
      }
      return (time1 * operationCount1 + time2 * operationCount2)
          / (operationCount1 + operationCount2);
    }

    private static float millisecondBetween(Timestamp from, Timestamp to) {
      // The time unit we want is millisecond.
      // 1 second = 1000 milliseconds
      // 1 millisecond = 1000,000 nanoseconds
      Duration d = Timestamps.between(from, to);
      return d.getSeconds() * 1000.0f + d.getNanos() / (1000.0f * 1000.0f);
    }
  }
}
