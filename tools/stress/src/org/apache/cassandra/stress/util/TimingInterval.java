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


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.HdrHistogram.Histogram;

// represents measurements taken over an interval of time
// used for both single timer results and merged timer results
public final class TimingInterval
{
    // nanos
    private final long start;
    private final long end;
    public final long pauseLength;
    public final long pauseStart;

    // discrete
    public final long partitionCount;
    public final long rowCount;
    public final long operationCount;

    public final Histogram expectedTimesHistogram;
    public final Histogram actualTimesHistogram;


    TimingInterval(long time)
    {
        start = end = time;
        partitionCount = rowCount = operationCount = 0;
        pauseStart = pauseLength = 0;
        expectedTimesHistogram = new Histogram(3);
        actualTimesHistogram = new Histogram(3);
    }

    TimingInterval(long start, long end, long pauseStart, long pauseLength, long partitionCount, long rowCount, long operationCount, Histogram expectedTimesHistogram, Histogram actualTimesHistogram)
    {
        this.start = start;
        this.end = Math.max(end, start);
        this.partitionCount = partitionCount;
        this.rowCount = rowCount;
        this.operationCount = operationCount;
        this.pauseStart = pauseStart;
        this.pauseLength = pauseLength;
        this.actualTimesHistogram = actualTimesHistogram;
        this.expectedTimesHistogram = expectedTimesHistogram;
    }

    // merge multiple timer intervals together
    static TimingInterval merge(List<TimingInterval> intervals, int maxSamples, long start)
    {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long operationCount = 0, partitionCount = 0, rowCount = 0;
        long maxLatency = 0, totalLatency = 0;
        List<SampleOfLongs> latencies = new ArrayList<>();
        Histogram mergedExpectedTimesHistogram = new Histogram(3);
        Histogram mergedActualTimesHistogram = new Histogram(3);

        long startTime = Long.MAX_VALUE;
        long endTime = 0;

        long end = 0;
        long pauseStart = 0, pauseEnd = Long.MAX_VALUE;
        for (TimingInterval interval : intervals)
        {
            end = Math.max(end, interval.end);
            operationCount += interval.operationCount;
            partitionCount += interval.partitionCount;
            rowCount += interval.rowCount;
            mergedExpectedTimesHistogram.add(interval.expectedTimesHistogram);
            mergedActualTimesHistogram.add(interval.actualTimesHistogram);

            // track start and end time across all interval histograms:
            startTime = Math.min(startTime, interval.expectedTimesHistogram.getStartTimeStamp());
            startTime = Math.min(startTime, interval.actualTimesHistogram.getStartTimeStamp());
            endTime = Math.max(endTime, interval.expectedTimesHistogram.getEndTimeStamp());
            endTime = Math.max(endTime, interval.actualTimesHistogram.getEndTimeStamp());

            if (interval.pauseLength > 0)
            {
                pauseStart = Math.max(pauseStart, interval.pauseStart);
                pauseEnd = Math.min(pauseEnd, interval.pauseStart + interval.pauseLength);
            }
        }

        mergedActualTimesHistogram.setStartTimeStamp(startTime);
        mergedActualTimesHistogram.setEndTimeStamp(endTime);
        mergedExpectedTimesHistogram.setStartTimeStamp(startTime);
        mergedExpectedTimesHistogram.setEndTimeStamp(endTime);

        if (pauseEnd < pauseStart)
            pauseEnd = pauseStart = 0;

        return new TimingInterval(start, end, pauseStart, pauseEnd - pauseStart, partitionCount, rowCount, operationCount,
                mergedExpectedTimesHistogram, mergedActualTimesHistogram);

    }

    public double opRate()
    {
        return operationCount / ((end - start) * 0.000000001d);
    }

    public double adjustedRowRate()
    {
        return rowCount / ((end - (start + pauseLength)) * 0.000000001d);
    }

    public double partitionRate()
    {
        return partitionCount / ((end - start) * 0.000000001d);
    }

    public double rowRate()
    {
        return rowCount / ((end - start) * 0.000000001d);
    }

    public double meanLatency()
    {
        return expectedTimesHistogram.getMean() * 0.000001d;
    }

    public double actualTimesMeanLatency()
    {
        return actualTimesHistogram.getMean() * 0.000001d;
    }

    public double maxLatency()
    {
        return expectedTimesHistogram.getMaxValue() * 0.000001d;
    }

    public double actualTimesMaxLatency()
    {
        return actualTimesHistogram.getMaxValue() * 0.000001d;
    }

    public long runTime()
    {
        return (end - start) / 1000000;
    }

    public double medianLatency()
    {
        return expectedTimesHistogram.getValueAtPercentile(50.0) * 0.000001d;
    }

    public double actualTimesMedianLatency()
    {
        return actualTimesHistogram.getValueAtPercentile(50.0) * 0.000001d;
    }

    // 0 < rank < 1
    public double rankLatency(float rank)
    {
        return expectedTimesHistogram.getValueAtPercentile(rank * 100.0) * 0.000001d;
    }

    public double actualTimesRankLatency(float rank)
    {
        return actualTimesHistogram.getValueAtPercentile(rank * 100.0) * 0.000001d;
    }

    public final long endNanos()
    {
        return end;
    }

    public final long endMillis()
    {
        return end / 1000000;
    }

    public long startNanos()
    {
        return start;
    }

    public Histogram getExpectedTimesHistogram() {
        return expectedTimesHistogram;
    }

    public Histogram getActualTimesHistogram() {
        return actualTimesHistogram;
    }
}

