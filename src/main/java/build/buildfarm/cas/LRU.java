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

package build.buildfarm.cas;

import build.buildfarm.cas.CASFileCache.Entry;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

public interface LRU {

  void put(Entry e);

  Entry get(String key);

  Entry remove(String key);

  long getStorageSizeInBytes();

  boolean containsKey(String key);

  long totalEntryCount();

  long unreferencedEntryCount();

  void changeSize(long size);

  int decrementInputReferences(Iterable<String> inputFiles);

  Entry expireEntry(
      long blobSizeInBytes, ExecutorService service, Predicate<Entry> delegateInterrupted)
      throws InterruptedException;
}