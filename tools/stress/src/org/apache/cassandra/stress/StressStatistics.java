/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.stress;

import java.io.PrintStream;

import org.HdrHistogram.Histogram;
import org.apache.commons.lang3.time.DurationFormatUtils;

/**
 * Gathers and aggregates statistics for an operation
 */
public class StressStatistics
{
    private Session client;
    private PrintStream output;

    private long durationInSeconds;

    /** The sum of the interval_key_rate values collected by tallyAverages */
    private int tallyKeyRateSum;

    Histogram total = new Histogram(3);


    public StressStatistics(Session client, PrintStream out)
    {
        this.client = client;
        this.output = out;
    }

    /**
     * Collect statistics per-interval
     */
    public void addIntervalStats(int totalOperations,
                                 int intervalKeyRate,
                                 Histogram latency,
                                 long currentTimeInSeconds)
    {
        this.tallyAverages(totalOperations, intervalKeyRate,
                                latency, currentTimeInSeconds);
    }

    /**
     * Collect interval_op_rate and interval_key_rate averages
     */
    private void tallyAverages(int totalOperations,
                               int intervalKeyRate,
                               Histogram latency,
                               long currentTimeInSeconds)
    {
        //Skip the first and last 10% of values.
        //The middle values of the operation are the ones worthwhile
        //to collect and average:
        if (totalOperations > (0.10 * client.getNumKeys()) &&
            totalOperations < (0.90 * client.getNumKeys())) {
                tallyKeyRateSum += intervalKeyRate;
                if (total == null)
                    total = latency;
                else
                    total.add(latency);
            }
        durationInSeconds = currentTimeInSeconds;
    }

    public void printStats()
    {
        output.println("\n");
        if (total != null && total.getTotalCount() > 0) {
            double durationInSec = (total.getEndTimeStamp() - total.getStartTimeStamp())/1000.0;
            output.println("Averages from the middle 80% of values:");
            final double avgOpPerSec = total.getTotalCount() / durationInSec;
            output.printf("interval_op_rate          : %d \n", (int)avgOpPerSec);
            final double avgKeyPerSec = tallyKeyRateSum / durationInSec;
            output.printf("interval_key_rate         : %d \n", (int)avgKeyPerSec);
            output.printf("latency median            : %.1f \n", total.getValueAtPercentile(50.0)/1000000.0);
            output.printf("latency 95th percentile   : %.1f \n", total.getValueAtPercentile(95.0)/1000000.0);
            output.printf("latency 99.9th percentile : %.1f \n", total.getValueAtPercentile(99.9)/1000000.0);
            output.printf("Total duration            : %.2f \n",durationInSec);
        }
        output.println("Total operation time      : " + DurationFormatUtils.formatDuration(
            durationInSeconds*1000, "HH:mm:ss", true));
    }

}
