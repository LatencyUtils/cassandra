package org.apache.cassandra.stress.settings;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.stress.util.ThriftClient;
import org.apache.cassandra.stress.util.Timer;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.IndexClause;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.thrift.TException;

@SuppressWarnings("deprecation")
public final class DummyThriftClient implements ThriftClient {
    private final long delayNs;

    public DummyThriftClient(long delay_µs) {
        this.delayNs = delay_µs * 1000;
    }

    @Override
    public Integer prepare_cql_query(String query, Compression compression) throws InvalidRequestException, TException {
        return query.hashCode();
    }

    @Override
    public Integer prepare_cql3_query(String query, Compression compression) throws InvalidRequestException, TException {
        return query.hashCode();
    }

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> multiget_slice(List<ByteBuffer> keys, ColumnParent column_parent,
            SlicePredicate predicate, ConsistencyLevel consistency_level) throws InvalidRequestException,
            UnavailableException, TimedOutException, TException {
        delay();
        return null;
    }

    @Override
    public void insert(ByteBuffer key, ColumnParent column_parent, Column column, ConsistencyLevel consistency_level)
            throws InvalidRequestException, UnavailableException, TimedOutException, TException {
        delay();
    }

    @Override
    public List<ColumnOrSuperColumn> get_slice(ByteBuffer key, ColumnParent parent, SlicePredicate predicate,
            ConsistencyLevel consistencyLevel) throws InvalidRequestException, UnavailableException, TimedOutException,
            TException {
        delay();
        return null;
    }

    @Override
    public List<KeySlice> get_range_slices(ColumnParent column_parent, SlicePredicate predicate, KeyRange range,
            ConsistencyLevel consistency_level) throws InvalidRequestException, UnavailableException,
            TimedOutException, TException {
        delay();
        return null;
    }

    @Override
    public List<KeySlice> get_indexed_slices(ColumnParent column_parent, IndexClause index_clause,
            SlicePredicate column_predicate, ConsistencyLevel consistency_level) throws InvalidRequestException,
            UnavailableException, TimedOutException, TException {
        delay();
        return null;
    }

    @Override
    public CqlResult execute_prepared_cql_query(int itemId, ByteBuffer key, List<ByteBuffer> values)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
            TException {
        delay();
        return null;
    }

    @Override
    public CqlResult execute_prepared_cql3_query(int itemId, ByteBuffer key, List<ByteBuffer> values,
            ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException, TException {
        delay();
        return null;
    }

    @Override
    public CqlResult execute_cql_query(String query, ByteBuffer key, Compression compression)
            throws InvalidRequestException, UnavailableException, TimedOutException, SchemaDisagreementException,
            TException {
        delay();
        return null;
    }

    @Override
    public CqlResult execute_cql3_query(String query, ByteBuffer key, Compression compression,
            ConsistencyLevel consistency) throws InvalidRequestException, UnavailableException, TimedOutException,
            SchemaDisagreementException, TException {
        delay();
        return null;
    }

    @Override
    public void batch_mutate(Map<ByteBuffer, Map<String, List<Mutation>>> record, ConsistencyLevel consistencyLevel)
            throws TException {
        delay();
    }

    private void delay() {
        if(delayNs > 0)
            Timer.sleepNs(delayNs);
    }
}