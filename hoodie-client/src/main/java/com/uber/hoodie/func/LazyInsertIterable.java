/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.func;

import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.io.HoodieCreateHandle;
import com.uber.hoodie.io.HoodieIOHandle;
import com.uber.hoodie.table.HoodieTable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import scala.Tuple2;

/**
 * Lazy Iterable, that writes a stream of HoodieRecords sorted by the partitionPath,
 * into new files.
 */
public class LazyInsertIterable<T extends HoodieRecordPayload> extends LazyIterableIterator<HoodieRecord<T>, List<WriteStatus>> {

    private static Logger logger = LogManager.getLogger(LazyInsertIterable.class);
    private final HoodieWriteConfig hoodieConfig;
    private final String commitTime;
    private final HoodieTable<T> hoodieTable;
    private Set<String> partitionsCleaned;
    private HoodieCreateHandle handle;

    public LazyInsertIterable(Iterator<HoodieRecord<T>> sortedRecordItr, HoodieWriteConfig config,
        String commitTime, HoodieTable<T> hoodieTable) {
        super(sortedRecordItr);
        this.partitionsCleaned = new HashSet<>();
        this.hoodieConfig = config;
        this.commitTime = commitTime;
        this.hoodieTable = hoodieTable;
    }

    @Override protected void start() {
    }

    static class RecordWrapper {
        HoodieRecord hoodieRecord;

        public RecordWrapper(HoodieRecord hoodieRecord) {
            this.hoodieRecord = hoodieRecord;
        }
    }

    @Override protected List<WriteStatus> computeNext()  {
        List<WriteStatus> statuses = new ArrayList<>();
        AtomicLong timeTakenToFetchRecord = new AtomicLong(0);
        AtomicLong timeTakenToPreProcess = new AtomicLong(0);
        AtomicLong timeTakenToWrite = new AtomicLong(0);
        LinkedBlockingQueue<RecordWrapper> recordQ = new LinkedBlockingQueue<>(512);
        LinkedBlockingQueue<Boolean> isDone = new LinkedBlockingQueue<>();
        new Thread(() -> {
            while (inputItr.hasNext()) {
                HoodieRecord record = inputItr.next();
                insertRecord(recordQ, new RecordWrapper(record));
            }
            insertRecord(recordQ, new RecordWrapper(null));
        }).start();

        while (true) {
            long startFetchRecord = System.nanoTime();
            RecordWrapper recordWrapper;
            try {
                recordWrapper = recordQ.take();
            } catch (InterruptedException e) {
                isDone.offer(true);
                throw new RuntimeException(e);
            }
            if (recordWrapper.hoodieRecord == null) {
                break;
            }
            HoodieRecord record = recordWrapper.hoodieRecord;
            long endFetchRecord = System.nanoTime();
            timeTakenToFetchRecord.addAndGet(endFetchRecord - startFetchRecord);

            // clean up any partial failures
            if (!partitionsCleaned.contains(record.getPartitionPath())) {
                // This insert task could fail multiple times, but Spark will faithfully retry with
                // the same data again. Thus, before we open any files under a given partition, we
                // first delete any files in the same partitionPath written by same Spark partition
                HoodieIOHandle.cleanupTmpFilesFromCurrentCommit(hoodieConfig,
                    commitTime,
                    record.getPartitionPath(),
                    TaskContext.getPartitionId());
                partitionsCleaned.add(record.getPartitionPath());
            }

            // lazily initialize the handle, for the first time
            if (handle == null) {
                handle =
                    new HoodieCreateHandle(hoodieConfig, commitTime, hoodieTable,
                        record.getPartitionPath());
            }

            if (handle.canWrite(record)) {
                // write the record, if the handle has capacity
                final Tuple2<Long, Long> writeTime = handle.write(record);
                timeTakenToPreProcess.addAndGet(writeTime._1());
                timeTakenToWrite.addAndGet(writeTime._2());
            } else {
                // handle is full.
                statuses.add(handle.close());
                // Need to handle the rejected record & open new handle
                handle =
                    new HoodieCreateHandle(hoodieConfig, commitTime, hoodieTable,
                        record.getPartitionPath());
                handle.write(record); // we should be able to write 1 record.
                break;
            }
        }

        // If we exited out, because we ran out of records, just close the pending handle.
        if (!inputItr.hasNext()) {
            if (handle != null) {
                statuses.add(handle.close());
            }
        }
        logger.info("time to fetch :" + TimeUnit.NANOSECONDS.toSeconds(timeTakenToFetchRecord.get())
            + " time to process :" + TimeUnit.NANOSECONDS.toSeconds(timeTakenToPreProcess.get())
            + " time to write :" + TimeUnit.NANOSECONDS.toSeconds(timeTakenToWrite.get()));
        assert statuses.size() > 0; // should never return empty statuses
        return statuses;
    }

    private void insertRecord(final LinkedBlockingQueue<RecordWrapper> recordQ, final RecordWrapper record) {
        while(true) {
            try {
                if (!recordQ.offer(record, 10, TimeUnit.SECONDS)) {
                    continue;
                }
                break;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override protected void end() {

    }
}
