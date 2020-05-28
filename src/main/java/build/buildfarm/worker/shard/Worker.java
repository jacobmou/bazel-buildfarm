// Copyright 2017 The Bazel Authors. All rights reserved.
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

import static build.buildfarm.cas.ContentAddressableStorages.createGrpcCAS;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINER;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.MemoryCAS;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.DigestUtil.HashFunction;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.LoggingMain;
import build.buildfarm.common.ShardBackplane;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.RedisShardBackplane;
import build.buildfarm.instance.shard.RemoteInputStreamFactory;
import build.buildfarm.instance.shard.WorkerStubs;
import build.buildfarm.server.ByteStreamService;
import build.buildfarm.server.ContentAddressableStorageService;
import build.buildfarm.server.Instances;
import build.buildfarm.v1test.ContentAddressableStorageConfig;
import build.buildfarm.v1test.FilesystemCASConfig;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.ShardWorkerConfig;
import build.buildfarm.worker.CASFileCache;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.FuseCAS;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import build.buildfarm.worker.WorkerPeriodicProfile;
import com.google.common.base.Strings;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.devtools.common.options.OptionsParser;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.logging.Logger;
import javax.naming.ConfigurationException;

public class Worker extends LoggingMain {
  private static final java.util.logging.Logger nettyLogger =
      java.util.logging.Logger.getLogger("io.grpc.netty");
  private static final Logger logger = Logger.getLogger(Worker.class.getName());

  private static final int shutdownWaitTimeInSeconds = 10;

  private final ShardWorkerConfig config;
  private final ShardWorkerInstance instance;
  private final Server server;
  private final Path root;
  private final DigestUtil digestUtil;
  private final ExecFileSystem execFileSystem;
  private final Pipeline pipeline;
  private final ShardBackplane backplane;
  private final LoadingCache<String, Instance> workerStubs;

  public Worker(String session, ShardWorkerConfig config) throws ConfigurationException {
    this(session, ServerBuilder.forPort(config.getPort()), config);
  }

  private static Path getValidRoot(ShardWorkerConfig config) throws ConfigurationException {
    String rootValue = config.getRoot();
    if (Strings.isNullOrEmpty(rootValue)) {
      throw new ConfigurationException("root value in config missing");
    }
    Path root = Paths.get(rootValue);
    if (!Files.isDirectory(root)) {
      throw new ConfigurationException("root [" + root.toString() + "] is not directory");
    }
    return root;
  }

  private static Path getValidFilesystemCASPath(FilesystemCASConfig config, Path root)
      throws ConfigurationException {
    String pathValue = config.getPath();
    if (Strings.isNullOrEmpty(pathValue)) {
      throw new ConfigurationException("Cas cache directory value in config missing");
    }
    return root.resolve(pathValue);
  }

  private static HashFunction getValidHashFunction(ShardWorkerConfig config)
      throws ConfigurationException {
    try {
      return HashFunction.get(config.getDigestFunction());
    } catch (IllegalArgumentException e) {
      throw new ConfigurationException("hash_function value unrecognized");
    }
  }

  private Operation stripOperation(Operation operation) {
    return instance.stripOperation(operation);
  }

  private Operation stripQueuedOperation(Operation operation) {
    return instance.stripQueuedOperation(operation);
  }

  public Worker(String session, ServerBuilder<?> serverBuilder, ShardWorkerConfig config)
      throws ConfigurationException {
    super("BuildFarmShardWorker");
    this.config = config;
    String identifier = "buildfarm-worker-" + config.getPublicName() + "-" + session;
    root = getValidRoot(config);
    if (config.getPublicName().isEmpty()) {
      throw new ConfigurationException("worker's public name should not be empty");
    }

    digestUtil = new DigestUtil(getValidHashFunction(config));

    ShardWorkerConfig.BackplaneCase backplaneCase = config.getBackplaneCase();
    switch (backplaneCase) {
      default:
      case BACKPLANE_NOT_SET:
        throw new IllegalArgumentException("Shard Backplane not set in config");
      case REDIS_SHARD_BACKPLANE_CONFIG:
        backplane =
            new RedisShardBackplane(
                config.getRedisShardBackplaneConfig(),
                identifier,
                this::stripOperation,
                this::stripQueuedOperation,
                (o) -> false,
                (o) -> false);
        break;
    }

    workerStubs = WorkerStubs.create(digestUtil);

    ExecutorService removeDirectoryService =
        newFixedThreadPool(
            /* nThreads=*/ 32,
            new ThreadFactoryBuilder().setNameFormat("remove-directory-pool-%d").build());
    ExecutorService accessRecorder = newSingleThreadExecutor();

    InputStreamFactory remoteInputStreamFactory =
        new RemoteInputStreamFactory(
            config.getPublicName(),
            backplane,
            new Random(),
            workerStubs,
            (worker, t, context) -> {});
    ContentAddressableStorage storage =
        createStorages(
            remoteInputStreamFactory, removeDirectoryService, accessRecorder, config.getCasList());
    execFileSystem =
        createExecFileSystem(
            remoteInputStreamFactory, removeDirectoryService, accessRecorder, storage);

    instance =
        new ShardWorkerInstance(
            config.getPublicName(),
            digestUtil,
            backplane,
            storage,
            execFileSystem,
            config.getShardWorkerInstanceConfig());

    Instances instances = Instances.singular(instance);

    ShardWorkerContext context =
        new ShardWorkerContext(
            config.getPublicName(),
            config.getPlatform(),
            config.getOperationPollPeriod(),
            backplane::pollOperation,
            config.getInlineContentLimit(),
            config.getInputFetchStageWidth(),
            config.getExecuteStageWidth(),
            backplane,
            execFileSystem,
            new EmptyInputStreamFactory(
                new FailoverInputStreamFactory(
                    execFileSystem.getStorage(), remoteInputStreamFactory)),
            config.getExecutionPoliciesList(),
            instance,
            /* deadlineAfter=*/ 1,
            /* deadlineAfterUnits=*/ DAYS,
            config.getDefaultActionTimeout(),
            config.getMaximumActionTimeout(),
            config.getLimitExecution(),
            config.getLimitGlobalExecution(),
            config.getOnlyMulticoreTests());

    WorkerPeriodicProfile workerProfile = WorkerPeriodicProfile.create();
    PipelineStage completeStage =
        new PutOperationStage((operation) -> context.deactivate(operation.getName()), workerProfile);
    PipelineStage errorStage = completeStage; /* new ErrorStage(); */
    PipelineStage reportResultStage = new ReportResultStage(context, completeStage, errorStage);
    PipelineStage executeActionStage =
        new ExecuteActionStage(context, reportResultStage, errorStage);
    PipelineStage inputFetchStage =
        new InputFetchStage(context, executeActionStage, new PutOperationStage(context::requeue, workerProfile));
    PipelineStage matchStage = new MatchStage(context, inputFetchStage, errorStage);

    pipeline = new Pipeline();
    // pipeline.add(errorStage, 0);
    pipeline.add(matchStage, 4);
    pipeline.add(inputFetchStage, 3);
    pipeline.add(executeActionStage, 2);
    pipeline.add(reportResultStage, 1);

    server =
        serverBuilder
            .addService(
                new ContentAddressableStorageService(
                    instances, /* deadlineAfter=*/ 1, DAYS, /* requestLogLevel=*/ FINER))
            .addService(new ByteStreamService(instances, /* writeDeadlineAfter=*/ 1, DAYS))
            .addService(
                new WorkerProfileService(
                    storage, inputFetchStage, executeActionStage, context, workerProfile))
            .build();

    logger.log(INFO, String.format("%s initialized", identifier));
  }

  private ExecFileSystem createFuseExecFileSystem(
      InputStreamFactory remoteInputStreamFactory, ContentAddressableStorage storage) {
    InputStreamFactory storageInputStreamFactory =
        (digest, offset) -> storage.get(digest).getData().substring((int) offset).newInput();

    InputStreamFactory localPopulatingInputStreamFactory =
        new InputStreamFactory() {
          @Override
          public InputStream newInput(Digest blobDigest, long offset)
              throws IOException, InterruptedException {
            // FIXME use write
            ByteString content =
                ByteString.readFrom(remoteInputStreamFactory.newInput(blobDigest, offset));

            if (offset == 0) {
              // extra computations
              Blob blob = new Blob(content, digestUtil);
              // here's hoping that our digest matches...
              storage.put(blob);
            }

            return content.newInput();
          }
        };
    return new FuseExecFileSystem(
        root,
        new FuseCAS(
            root,
            new EmptyInputStreamFactory(
                new FailoverInputStreamFactory(
                    storageInputStreamFactory, localPopulatingInputStreamFactory))),
        storage);
  }

  private ExecFileSystem createExecFileSystem(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      ContentAddressableStorage storage) {
    checkState(storage != null, "no exec fs cas specified");
    if (storage instanceof CASFileCache) {
      return createCFCExecFileSystem(
          removeDirectoryService, accessRecorder, (CASFileCache) storage);
    } else {
      // FIXME not the only fuse backing capacity...
      return createFuseExecFileSystem(remoteInputStreamFactory, storage);
    }
  }

  private ContentAddressableStorage createStorage(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      Executor accessRecorder,
      ContentAddressableStorageConfig config,
      ContentAddressableStorage delegate)
      throws ConfigurationException {
    switch (config.getTypeCase()) {
      default:
      case TYPE_NOT_SET:
        throw new IllegalArgumentException("Invalid cas type specified");
      case MEMORY:
      case FUSE: // FIXME have FUSE refer to a name for storage backing, and topo
        return new MemoryCAS(config.getMemory().getMaxSizeBytes(), this::onStoragePut, delegate);
      case GRPC:
        checkState(delegate == null, "grpc cas cannot delegate");
        return createGrpcCAS(config.getGrpc());
      case FILESYSTEM:
        FilesystemCASConfig fsCASConfig = config.getFilesystem();
        return new ShardCASFileCache(
            remoteInputStreamFactory,
            root.resolve(getValidFilesystemCASPath(fsCASConfig, root)),
            fsCASConfig.getMaxSizeBytes(),
            fsCASConfig.getMaxEntrySizeBytes(),
            digestUtil,
            removeDirectoryService,
            accessRecorder,
            this::onStoragePut,
            delegate == null ? this::onStorageExpire : (digests) -> {},
            delegate);
    }
  }

  private ContentAddressableStorage createStorages(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      Executor accessRecorder,
      List<ContentAddressableStorageConfig> configs)
      throws ConfigurationException {
    ImmutableList.Builder<ContentAddressableStorage> storages = ImmutableList.builder();
    // must construct delegates first
    ContentAddressableStorage storage = null, delegate = null;
    for (ContentAddressableStorageConfig config : Lists.reverse(configs)) {
      storage =
          createStorage(
              remoteInputStreamFactory, removeDirectoryService, accessRecorder, config, delegate);
      storages.add(storage);
      delegate = storage;
    }
    return storage;
  }

  private ExecFileSystem createCFCExecFileSystem(
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      CASFileCache fileCache) {
    return new CFCExecFileSystem(
        root,
        fileCache,
        config.getLinkInputDirectories(),
        removeDirectoryService,
        accessRecorder,
        /* deadlineAfter=*/ 1,
        /* deadlineAfterUnits=*/ DAYS);
  }

  public void stop() throws InterruptedException {
    boolean interrupted = Thread.interrupted();
    logger.log(INFO, "Closing the pipeline");
    try {
      pipeline.close();
    } catch (InterruptedException e) {
      Thread.interrupted();
      interrupted = true;
    }
    logger.log(INFO, "Stopping exec filesystem");
    execFileSystem.stop();
    if (server != null) {
      logger.log(INFO, "Shutting down the server");
      server.shutdown();

      try {
        server.awaitTermination(shutdownWaitTimeInSeconds, SECONDS);
      } catch (InterruptedException e) {
        interrupted = true;
        logger.log(SEVERE, "interrupted while waiting for server shutdown", e);
      } finally {
        server.shutdownNow();
      }
    }
    try {
      backplane.stop();
    } catch (InterruptedException e) {
      interrupted = true;
    }
    workerStubs.invalidateAll();
    if (interrupted) {
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    }
  }

  private void onStoragePut(Digest digest) {
    try {
      backplane.addBlobLocation(digest, config.getPublicName());
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private void onStorageExpire(Iterable<Digest> digests) {
    try {
      backplane.removeBlobsLocation(digests, config.getPublicName());
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    // should really be waiting for either server or pipeline shutdown
    try {
      pipeline.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    stop();
  }

  private void removeWorker(String name) {
    try {
      backplane.removeWorker(name, "removing self prior to initialization");
    } catch (IOException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
        throw status.asRuntimeException();
      }
      logger.log(INFO, "backplane was unavailable or overloaded, deferring removeWorker");
    }
  }

  private void addBlobsLocation(List<Digest> digests, String name) {
    while (!backplane.isStopped()) {
      try {
        backplane.addBlobsLocation(digests, name);
        return;
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw status.asRuntimeException();
        }
      }
    }
    throw Status.UNAVAILABLE.withDescription("backplane was stopped").asRuntimeException();
  }

  private void addWorker(ShardWorker worker) {
    while (!backplane.isStopped()) {
      try {
        backplane.addWorker(worker);
        return;
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw status.asRuntimeException();
        }
      }
    }
    throw Status.UNAVAILABLE.withDescription("backplane was stopped").asRuntimeException();
  }

  private void startFailsafeRegistration() {
    String endpoint = config.getPublicName();
    ShardWorker.Builder worker = ShardWorker.newBuilder().setEndpoint(endpoint);
    int registrationIntervalMillis = 10000;
    int registrationOffsetMillis = registrationIntervalMillis * 3;
    new Thread(
            new Runnable() {
              long workerRegistrationExpiresAt = 0;

              ShardWorker nextRegistration(long now) {
                return worker.setExpireAt(now + registrationOffsetMillis).build();
              }

              long nextInterval(long now) {
                return now + registrationIntervalMillis;
              }

              void registerIfExpired() {
                long now = System.currentTimeMillis();
                if (now >= workerRegistrationExpiresAt) {
                  // worker must be registered to match
                  addWorker(nextRegistration(now));
                  // update every 10 seconds
                  workerRegistrationExpiresAt = nextInterval(now);
                }
              }

              @Override
              public void run() {
                try {
                  while (!server.isShutdown()) {
                    registerIfExpired();
                    SECONDS.sleep(1);
                  }
                } catch (InterruptedException e) {
                  try {
                    stop();
                  } catch (InterruptedException ie) {
                    logger.log(SEVERE, "interrupted while stopping worker", ie);
                    // ignore
                  }
                }
              }
            })
        .start();
  }

  public void start() throws InterruptedException {
    try {
      backplane.start();

      removeWorker(config.getPublicName());

      execFileSystem.start((digests) -> addBlobsLocation(digests, config.getPublicName()));

      server.start();
      startFailsafeRegistration();
    } catch (Exception e) {
      stop();
      logger.log(SEVERE, "error starting worker", e);
      return;
    }
    pipeline.start();
  }

  @Override
  protected void onShutdown() throws InterruptedException {
    logger.log(SEVERE, "*** shutting down gRPC server since JVM is shutting down");
    stop();
    logger.log(SEVERE, "*** server shut down");
  }

  private static ShardWorkerConfig toShardWorkerConfig(Readable input, WorkerOptions options)
      throws IOException {
    ShardWorkerConfig.Builder builder = ShardWorkerConfig.newBuilder();
    TextFormat.merge(input, builder);
    if (!Strings.isNullOrEmpty(options.root)) {
      builder.setRoot(options.root);
    }
    if (!Strings.isNullOrEmpty(options.publicName)) {
      builder.setPublicName(options.publicName);
    }
    return builder.build();
  }

  private static void printUsage(OptionsParser parser) {
    logger.log(INFO, "Usage: CONFIG_PATH");
    logger.log(
        INFO, parser.describeOptions(Collections.emptyMap(), OptionsParser.HelpVerbosity.LONG));
  }

  public static void main(String[] args) {
    try {
      startWorker(args);
    } catch (Exception e) {
      logger.log(SEVERE, "exception caught", e);
      System.exit(1);
    }
  }

  public static void startWorker(String[] args) throws Exception {
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(SEVERE);

    OptionsParser parser = OptionsParser.newOptionsParser(WorkerOptions.class);
    parser.parseAndExitUponError(args);
    List<String> residue = parser.getResidue();
    if (residue.isEmpty()) {
      printUsage(parser);
      throw new IllegalArgumentException("Missing CONFIG_PATH");
    }
    Path configPath = Paths.get(residue.get(0));
    String session = UUID.randomUUID().toString();
    Worker worker;
    try (InputStream configInputStream = Files.newInputStream(configPath)) {
      worker =
          new Worker(
              session,
              toShardWorkerConfig(
                  new InputStreamReader(configInputStream),
                  parser.getOptions(WorkerOptions.class)));
    }
    worker.start();
    worker.blockUntilShutdown();
    System.exit(0); // bullet to the head in case anything is stuck
  }
}
