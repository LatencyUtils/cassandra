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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.io.PrintStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.stress.operations.CounterAdder;
import org.apache.cassandra.stress.operations.CounterGetter;
import org.apache.cassandra.stress.operations.CqlCounterAdder;
import org.apache.cassandra.stress.operations.CqlCounterGetter;
import org.apache.cassandra.stress.operations.CqlIndexedRangeSlicer;
import org.apache.cassandra.stress.operations.CqlInserter;
import org.apache.cassandra.stress.operations.CqlMultiGetter;
import org.apache.cassandra.stress.operations.CqlRangeSlicer;
import org.apache.cassandra.stress.operations.CqlReader;
import org.apache.cassandra.stress.operations.IndexedRangeSlicer;
import org.apache.cassandra.stress.operations.Inserter;
import org.apache.cassandra.stress.operations.MultiGetter;
import org.apache.cassandra.stress.operations.RangeSlicer;
import org.apache.cassandra.stress.operations.Reader;
import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.stress.util.Operation;
import org.apache.cassandra.transport.SimpleClient;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;
import com.yammer.metrics.stats.Snapshot;

public class StressAction extends Thread
{
    class OperationStream {
        private final AtomicInteger operationIndex = new AtomicInteger(0);
        private final long start = System.nanoTime();
        private final long intervalNs;
        public OperationStream(long intervalNs) {
            this.intervalNs = intervalNs;
        }

        Operation take() {
            // replaces the following lines:
            //            rateLimiter.acquire(); // throttles when there's a fixed rate set
            //            operations.take(); // blocks when operations queue is empty

            // all we need is operation index to determine the next operation, so claiming the index is all the synchronization we need
            int i = operationIndex.getAndIncrement();

            // this is the stopping condition for the producer
            if (i > client.getNumKeys()) {
                // in this case the consumers block forever, so... block forever
                while(true) {
                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    } catch (InterruptedException e) {
                    }
                }
            }

            // create op based on index
            Operation o = createOperation(i % client.getNumDifferentKeys());

            // if there's no throttling, there's no intended time for the operation and no need to sleep
            if (intervalNs != 0) {
                final long intendedTime = start + i * intervalNs;
                final long now = System.nanoTime();
                final long sleepFor = intendedTime - now;
                o.setIntendedTime(intendedTime);
                if(sleepFor > 0) {
                    Uninterruptibles.sleepUninterruptibly(sleepFor, TimeUnit.NANOSECONDS);
                }
            }
            return o;
        }
    }
    private OperationStream operations;

    private final Session client;
    private final PrintStream output;

    private volatile boolean stop = false;

    public static final int SUCCESS = 0;
    public static final int FAILURE = 1;

    private volatile int returnCode = -1;

    public StressAction(Session session, PrintStream out)
    {
        client = session;
        output = out;
    }

    public void run()
    {
        Snapshot responseTime, serviceTime;
        long oldLatency;
        int epoch, total, oldTotal, keyCount, oldKeyCount;

        // creating keyspace and column families
        if (client.getOperation() == Stress.Operations.INSERT || client.getOperation() == Stress.Operations.COUNTER_ADD)
            client.createKeySpaces();

        final double maxOpsPerSecond = client.getMaxOpsPerSecond();
        if (maxOpsPerSecond != Double.MAX_VALUE) {
            operations = new OperationStream((long) (SECONDS.toNanos(1)/maxOpsPerSecond));
        }
        else {
            operations = new OperationStream(0);
        }
        int threadCount = client.getThreads();
        Consumer[] consumers = new Consumer[threadCount];

        output.println("total,interval_op_rate,interval_key_rate,latency,95th,99.9th,elapsed_time");

        int itemsPerThread = client.getKeysPerThread();
        int modulo = client.getNumKeys() % threadCount;
        RateLimiter rateLimiter = RateLimiter.create(maxOpsPerSecond);

        // creating required type of the threads for the test
        for (int i = 0; i < threadCount; i++) {
            if (i == threadCount - 1)
                itemsPerThread += modulo; // last one is going to handle N + modulo items

            consumers[i] = new Consumer(itemsPerThread, rateLimiter);
        }

//        Producer producer = new Producer();
//        producer.start();

        // starting worker threads
        for (int i = 0; i < threadCount; i++)
            consumers[i].start();

        // initialization of the values
        boolean terminate = false;
        epoch = total = keyCount = 0;

        int interval = client.getProgressInterval();
        int epochIntervals = client.getProgressInterval() * 10;
        long testStartTime = System.nanoTime();

        StressStatistics stats = new StressStatistics(client, output);

        while (!terminate)
        {
            if (stop)
            {
//                producer.stopProducer();

                for (Consumer consumer : consumers)
                    consumer.stopConsume();

                break;
            }

            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            int alive = 0;
            for (Thread thread : consumers)
                if (thread.isAlive()) alive++;

            if (alive == 0)
                terminate = true;

            epoch++;

            if (terminate || epoch > epochIntervals)
            {
                epoch = 0;

                oldTotal = total;
                oldKeyCount = keyCount;

                total = client.operations.get();
                keyCount = client.keys.get();
                responseTime = client.responseTime.getSnapshot();
                serviceTime = client.serviceTime.getSnapshot();

                int opDelta = total - oldTotal;
                int keyDelta = keyCount - oldKeyCount;

                long currentTimeInSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - testStartTime);

                output.println(String.format("(rt)%d,%d,%d,%.1f,%.1f,%.1f,%d",
                                             total,
                                             opDelta / interval,
                                             keyDelta / interval,
                                             responseTime.getMedian(), responseTime.get95thPercentile(), responseTime.get999thPercentile(),
                                             currentTimeInSeconds));
                output.println(String.format("(st)%d,%d,%d,%.1f,%.1f,%.1f,%d",
                        total,
                        opDelta / interval,
                        keyDelta / interval,
                        serviceTime.getMedian(), serviceTime.get95thPercentile(), serviceTime.get999thPercentile(),
                        currentTimeInSeconds));

                if (client.outputStatistics()) {
                    stats.addIntervalStats(total,
                                           opDelta / interval,
                                           keyDelta / interval,
                                           responseTime,
                                           currentTimeInSeconds);
                        }
            }
        }

        // if any consumer failed, set the return code to failure.
        returnCode = SUCCESS;
//        if (producer.isAlive())
//        {
//            producer.interrupt(); // if producer is still alive it means that we had errors in the consumers
//            returnCode = FAILURE;
//        }
        for (Consumer consumer : consumers)
            if (consumer.getReturnCode() == FAILURE)
                returnCode = FAILURE;

        if (returnCode == SUCCESS) {
            if (client.outputStatistics())
                stats.printStats();
            // marking an end of the output to the client
            output.println("END");
        } else {
            output.println("FAILURE");
        }

    }

    public int getReturnCode()
    {
        return returnCode;
    }

//    /**
//     * Produces exactly N items (awaits each to be consumed)
//     */
//    private class Producer extends Thread
//    {
//        private volatile boolean stop = false;
//
//        public void run()
//        {
//            for (int i = 0; i < client.getNumKeys(); i++)
//            {
//                if (stop)
//                    break;
//
//                try
//                {
//                    operations.put(createOperation(i % client.getNumDifferentKeys()));
//                }
//                catch (InterruptedException e)
//                {
//                    if (e.getMessage() != null)
//                        System.err.println("Producer error - " + e.getMessage());
//                    return;
//                }
//            }
//        }
//
//        public void stopProducer()
//        {
//            stop = true;
//            this.interrupt();
//        }
//    }

    /**
     * Each consumes exactly N items from queue
     */
    private class Consumer extends Thread
    {
        private final int items;
//        private final RateLimiter rateLimiter;
        private volatile boolean stop = false;
        private volatile int returnCode = StressAction.SUCCESS;

        public Consumer(int toConsume, RateLimiter rateLimiter)
        {
            items = toConsume;
//            this.rateLimiter = rateLimiter;
        }

        public void run()
        {
            if (client.use_native_protocol)
            {
                SimpleClient connection = client.getNativeClient();

                for (int i = 0; i < items; i++)
                {
                    if (stop)
                        break;

                    try
                    {
                        final Operation op = operations.take();
                        op.run(connection); // running job
                        recordOpLatencies(op);
                    }
                    catch (Exception e)
                    {
                        if (output == null)
                        {
                            System.err.println(e.getMessage());
                            returnCode = StressAction.FAILURE;
                            System.exit(-1);
                        }

                        output.println(e.getMessage());
                        returnCode = StressAction.FAILURE;
                        break;
                    }
                }
            }
            else
            {
                CassandraClient connection = client.getClient();

                for (int i = 0; i < items; i++)
                {
                    if (stop)
                        break;

                    try
                    {
                        final Operation op = operations.take();
                        op.run(connection); // running job
                        recordOpLatencies(op);
                    }
                    catch (Exception e)
                    {
                        if (output == null)
                        {
                            System.err.println(e.getMessage());
                            returnCode = StressAction.FAILURE;
                            System.exit(-1);
                        }

                        output.println(e.getMessage());
                        returnCode = StressAction.FAILURE;
                        break;
                    }
                }
            }
        }

        private void recordOpLatencies(Operation op) {
            client.responseTime.update(op.responseTime(), TimeUnit.NANOSECONDS);
            client.serviceTime.update(op.serviceTime(), TimeUnit.NANOSECONDS);
        }

        public void stopConsume()
        {
            stop = true;
        }

        public int getReturnCode()
        {
            return returnCode;
        }
    }

    private Operation createOperation(int index)
    {
        switch (client.getOperation())
        {
            case READ:
                return client.isCQL() ? new CqlReader(client, index) : new Reader(client, index);

            case COUNTER_GET:
                return client.isCQL() ? new CqlCounterGetter(client, index) : new CounterGetter(client, index);

            case INSERT:
                return client.isCQL() ? new CqlInserter(client, index) : new Inserter(client, index);

            case COUNTER_ADD:
                return client.isCQL() ? new CqlCounterAdder(client, index) : new CounterAdder(client, index);

            case RANGE_SLICE:
                return client.isCQL() ? new CqlRangeSlicer(client, index) : new RangeSlicer(client, index);

            case INDEXED_RANGE_SLICE:
                return client.isCQL() ? new CqlIndexedRangeSlicer(client, index) : new IndexedRangeSlicer(client, index);

            case MULTI_GET:
                return client.isCQL() ? new CqlMultiGetter(client, index) : new MultiGetter(client, index);
        }

        throw new UnsupportedOperationException();
    }

    public void stopAction()
    {
        stop = true;
    }
}
