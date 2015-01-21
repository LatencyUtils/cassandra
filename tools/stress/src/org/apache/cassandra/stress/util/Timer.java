package org.apache.cassandra.stress.util;
/*
 * 
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
 * 
 */


import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

// a timer - this timer must be used by a single thread, and co-ordinates with other timers by
public final class Timer
{
    private ThreadLocalRandom rnd;

    // in progress snap start
    private long sampleStartNanos;
    private long expectedStartNanos;

    final Recorder actualTimesRecorder;
    final Recorder expectedTimesRecorder;
    private int opCount;

    // aggregate info
    private long partitionCount;
    private long rowCount;

    private long max;
    private long maxStart;
    private long upToDateAsOf;
    private long lastSnap = System.nanoTime();

    // communication with summary/logging thread
    private volatile CountDownLatch reportRequest;
    volatile TimingInterval report;
    private volatile TimingInterval finalReport;

    public Timer(int sampleCount)
    {
        this.actualTimesRecorder = new Recorder(3);
        this.expectedTimesRecorder = new Recorder(3);
    }

    public void init()
    {
        rnd = ThreadLocalRandom.current();
    }

    public void start() {
        // decide if we're logging this event
        sampleStartNanos = System.nanoTime();
    }

    public void expectedStart(long expectedStartNanos) {
        this.expectedStartNanos = expectedStartNanos;
    }

    public boolean running()
    {
        return finalReport == null;
    }

    public void stop(long partitionCount, long rowCount)
    {
        maybeReport();
        long now = System.nanoTime();
        actualTimesRecorder.recordValue(now - sampleStartNanos);
        expectedTimesRecorder.recordValue(now - expectedStartNanos);
        long time = now - expectedStartNanos;
        if (time > max) {
            maxStart = sampleStartNanos;
            max = time;
        }
        opCount += 1;
        this.partitionCount += partitionCount;
        this.rowCount += rowCount;
        upToDateAsOf = now;
    }

    private TimingInterval buildReport()
    {
        Histogram expectedTimesIntervalHistogram = expectedTimesRecorder.getIntervalHistogram();
        Histogram actualTimesIntervalHistogram = actualTimesRecorder.getIntervalHistogram();
        final TimingInterval report = new TimingInterval(lastSnap, upToDateAsOf, maxStart, actualTimesIntervalHistogram.getMaxValue(), partitionCount, rowCount, opCount,
                expectedTimesIntervalHistogram, actualTimesIntervalHistogram);
        // reset counters
        opCount = 0;
        partitionCount = 0;
        rowCount = 0;
        max = 0;
        lastSnap = upToDateAsOf;
        return report;
    }

    // checks to see if a report has been requested, and if so produces the report, signals and clears the request
    private void maybeReport()
    {
        if (reportRequest != null)
        {
            synchronized (this)
            {
                report = buildReport();
                reportRequest.countDown();
                reportRequest = null;
            }
        }
    }

    // checks to see if the timer is dead; if not requests a report, and otherwise fulfills the request itself
    synchronized void requestReport(CountDownLatch signal)
    {
        if (finalReport != null)
        {
            report = finalReport;
            finalReport = new TimingInterval(0);
            signal.countDown();
        }
        else
            reportRequest = signal;
    }

    // closes the timer; if a request is outstanding, it furnishes the request, otherwise it populates finalReport
    public synchronized void close()
    {
        if (reportRequest == null)
            finalReport = buildReport();
        else
        {
            finalReport = new TimingInterval(0);
            report = buildReport();
            reportRequest.countDown();
            reportRequest = null;
        }
    }
}