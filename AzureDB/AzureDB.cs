/*
 This file is part of AzureDB.
    AzureDB is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.
    AzureDB is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.
    You should have received a copy of the GNU General Public License
    along with AzureDB.  If not, see <http://www.gnu.org/licenses/>.
 * */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Auth;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Queue;

namespace AzureDB
{
    enum OpType
    {
        Upsert, Retrieve, RangeRetrieve, Nop, Delete
    }
    class AzureOperationHandle
    {
        public TaskCompletionSource<ScalableEntity> Task;
        public ScalableEntity Entity;
        public OpType Type;
        public ScalableDb.RetrieveCallback callback;
        public byte[] StartRange;
        public byte[] EndRange;
        public List<ScalableEntity> values = new List<ScalableEntity>();
        public AzureOperationHandle(ScalableEntity entity, OpType type)
        {
            Entity = entity;
            Task = new TaskCompletionSource<ScalableEntity>();
            Type = type;
        }
        public AzureOperationHandle SetValue(byte[] value)
        {
            Entity.Value = value;
            return this;
        }
        public AzureOperationHandle AddValue(ScalableEntity value)
        {
            values.Add(value);
            return this;
        }
    }
    class AzureEntity:TableEntity
    {
        public byte[] Value { get; set; }
        public AzureEntity()
        {
            ETag = "*";
        }
    }
    


    public class AzureDatabase:ScalableDb
    {
        

        CloudQueueClient qclient;
        CloudTableClient client;
        CloudTable table;
        Dictionary<ulong, List<AzureOperationHandle>> pendingOperations = new Dictionary<ulong, List<AzureOperationHandle>>();
        ManualResetEvent evt = new ManualResetEvent(false);
        bool running = true;
        System.Threading.Thread mthread;
        public AzureDatabase(string storageAccountString, string tableName)
        {
            
            CloudStorageAccount account = CloudStorageAccount.Parse(storageAccountString);
            qclient = account.CreateCloudQueueClient();
            client = account.CreateCloudTableClient();
            client.DefaultRequestOptions.PayloadFormat = TablePayloadFormat.JsonNoMetadata;
            table = client.GetTableReference(tableName);
            mthread = new Thread(async delegate () {
                await table.CreateIfNotExistsAsync();
                while (running)
                {

                    evt.WaitOne();
                    evt.Reset();
                    Dictionary<ulong, List<AzureOperationHandle>> ops;
                    lock (evt)
                    {
                        ops = pendingOperations;
                        pendingOperations = new Dictionary<ulong, List<AzureOperationHandle>>();
                    }

                    List<Task> runningTasks = new List<Task>();
                    foreach(var shard in ops)
                    {
                        TableBatchOperation upserts = new TableBatchOperation();
                        TableBatchOperation deletions = new TableBatchOperation();
                        Dictionary<ScalableEntity,List<AzureOperationHandle>> retrieves = new Dictionary<ScalableEntity, List<AzureOperationHandle>>(EntityComparer.instance);
                        Dictionary<ByteRange, List<AzureOperationHandle>> rangeRetrieves = new Dictionary<ByteRange, List<AzureOperationHandle>>();
                        foreach(var op in shard.Value.Where(m=>m.Type == OpType.Upsert || m.Type == OpType.Delete))
                        {
                            switch(op.Type)
                            {
                                case OpType.Upsert:
                                    upserts.Add(TableOperation.InsertOrReplace(new AzureEntity() { PartitionKey = op.Entity.Partition.ToString(), RowKey = Uri.EscapeDataString(Convert.ToBase64String(op.Entity.Key)), Value = op.Entity.Value }));
                                    if (upserts.Count == 100)
                                    {
                                        runningTasks.Add(table.ExecuteBatchAsync(upserts));
                                        upserts = new TableBatchOperation();
                                    }
                                    break;
                                case OpType.Delete:
                                    deletions.Add(TableOperation.Delete(new AzureEntity() { PartitionKey = op.Entity.Partition.ToString(), RowKey = Uri.EscapeDataString(Convert.ToBase64String(op.Entity.Key)), Value = op.Entity.Value }));
                                    if (deletions.Count == 100)
                                    {
                                        runningTasks.Add(table.ExecuteBatchAsync(deletions));
                                        deletions = new TableBatchOperation();
                                    }
                                    break;
                            }
                            
                        }
                        Func<IEnumerable<string>, string> and = (q) => {
                            string query = null;
                            foreach(string er in q.Where(m=>m != null))
                            {
                                if(query == null)
                                {
                                    query = er;
                                }else
                                {
                                    query = TableQuery.CombineFilters(query, TableOperators.And, er);
                                }
                            }
                            return query;
                        };
                        Func<IEnumerable<string>, string> or = (q) => {
                            string query = null;
                            foreach (string er in q.Where(m => m != null))
                            {
                                if (query == null)
                                {
                                    query = er;
                                }
                                else
                                {
                                    query = TableQuery.CombineFilters(query, TableOperators.Or, er);
                                }
                            }
                            return query;
                        };
                        Func<Dictionary<ScalableEntity,List<AzureOperationHandle>>,Dictionary<ByteRange,List<AzureOperationHandle>>,TableContinuationToken,TableQuery<AzureEntity>, Task> runSegmentedQuery = null;
                        runSegmentedQuery = async (tableops,rangeops,token, compiledQuery) => {
                            if(compiledQuery == null)
                            {
                                string query = null;
                                foreach(var iable in tableops.Values.SelectMany(m=>m).Where(m=>m.Type == OpType.Retrieve))
                                {
                                    if(query == null)
                                    {
                                        query = TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, iable.Entity.Partition.ToString()),TableOperators.And,TableQuery.GenerateFilterCondition("RowKey",QueryComparisons.Equal, Uri.EscapeDataString(Convert.ToBase64String(iable.Entity.Key))));
                                    }else
                                    {
                                        query = TableQuery.CombineFilters(query, TableOperators.Or, TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, iable.Entity.Partition.ToString()), TableOperators.And, TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, Uri.EscapeDataString(Convert.ToBase64String(iable.Entity.Key)))));
                                    }
                                }

                                foreach(var iable in rangeops.Values.SelectMany(m=>m).Where(m=>m.Type == OpType.RangeRetrieve))
                                {
                                    string startQuery = null;
                                    string endQuery = null;
                                    if(iable.StartRange != null)
                                    {
                                        startQuery = TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThan, new ScalableEntity(iable.StartRange, null) { UseLinearHash = true }.SetPartition(optimal_shard_size).Partition.ToString()),TableOperators.And,TableQuery.GenerateFilterCondition("RowKey",QueryComparisons.GreaterThan, Uri.EscapeDataString(Convert.ToBase64String(iable.StartRange))));
                                    }
                                    if (iable.EndRange != null)
                                    {
                                        endQuery = TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, new ScalableEntity(iable.EndRange, null) { UseLinearHash = true }.SetPartition(optimal_shard_size).Partition.ToString()), TableOperators.And, TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThan, Uri.EscapeDataString(Convert.ToBase64String(iable.EndRange))));
                                    }
                                    //TODO: Figure out how to get partitions to match what was inserted (all elements 0 to 100 have partition key 64)
                                    query = or(new string[] {query ,and(new string[] { startQuery, endQuery }) });
                                    

                                }
                                compiledQuery = new TableQuery<AzureEntity>().Where(query);
                            }
                            var segment = await table.ExecuteQuerySegmentedAsync(compiledQuery, token);
                            token = segment.ContinuationToken;
                            List<AzureOperationHandle> finished = new List<AzureOperationHandle>();
                            foreach (var iable in segment)
                            {
                                var ent = new ScalableEntity(Convert.FromBase64String(Uri.UnescapeDataString(iable.RowKey)),iable.Value);
                                if (tableops.ContainsKey(ent))
                                {
                                    finished.AddRange(tableops[ent].Select(m => m.SetValue(ent.Value)));
                                }
                                if(rangeops.Any())
                                {
                                    var range = new ByteRange(iable.Value, iable.Value);
                                    if (rangeops.ContainsKey(range))
                                    {
                                        finished.AddRange(rangeops[range].Select(m=>m.AddValue(new ScalableEntity(Convert.FromBase64String(Uri.UnescapeDataString(iable.RowKey)),iable.Value))));
                                    }
                                }
                            }
                            //Combine callbacks for finished queries
                            var callbacks = finished.ToLookup(m => m.callback);
                            foreach(var iable in callbacks)
                            {
                                if (!iable.Key(iable.Where(m=>m.Entity != null).Select(m => m.Entity).Union(iable.SelectMany(m=>m.values),EntityComparer.instance)))
                                {
                                    iable.AsParallel().ForAll(m => m.Type = OpType.Nop);
                                    compiledQuery = null; //Re-compile query
                                }

                            }
                            if (token != null)
                            {
                                await runSegmentedQuery(tableops,rangeops, token, compiledQuery);
                            }
                        };
                        foreach (var op in shard.Value.Where(m=>m.Type == OpType.Retrieve || m.Type == OpType.RangeRetrieve))
                        {
                            switch(op.Type)
                            {
                                case OpType.Retrieve:
                                    if (!retrieves.ContainsKey(op.Entity))
                                    {
                                        retrieves.Add(op.Entity, new List<AzureOperationHandle>());
                                    }
                                    retrieves[op.Entity].Add(op);
                                    if (retrieves.Count + rangeRetrieves.Count == 100)
                                    {
                                        runningTasks.Add(runSegmentedQuery(retrieves,rangeRetrieves, null, null));
                                        retrieves = new Dictionary<ScalableEntity, List<AzureOperationHandle>>(EntityComparer.instance);
                                        rangeRetrieves = new Dictionary<ByteRange, List<AzureOperationHandle>>();
                                    }
                                    break;
                                case OpType.RangeRetrieve:
                                    var ranger = new ByteRange(op.StartRange,op.EndRange);
                                    if (!rangeRetrieves.ContainsKey(ranger))
                                    {
                                        rangeRetrieves.Add(ranger, new List<AzureOperationHandle>());
                                    }
                                    rangeRetrieves[ranger].Add(op);
                                    if (retrieves.Count + rangeRetrieves.Count == 100)
                                    {
                                        runningTasks.Add(runSegmentedQuery(retrieves, rangeRetrieves, null, null));
                                        retrieves = new Dictionary<ScalableEntity, List<AzureOperationHandle>>(EntityComparer.instance);
                                        rangeRetrieves = new Dictionary<ByteRange, List<AzureOperationHandle>>();
                                    }
                                    break;
                            }
                            
                        }
                        foreach(var op in shard.Value.Where(m=>m.Type == OpType.Nop))
                        {
                            op.Task.SetResult(op.Entity);
                        }
                        if(upserts.Any())
                        {
                            runningTasks.Add(table.ExecuteBatchAsync(upserts));
                        }
                        if(retrieves.Any() || rangeRetrieves.Any())
                        {  
                            runningTasks.Add(runSegmentedQuery(retrieves,rangeRetrieves,null,null));
                        }
                    }
                    await Task.WhenAll(runningTasks);
                    ops.SelectMany(m => m.Value).AsParallel().ForAll(m => m.Task.SetResult(m.Entity));
                }
            });
            mthread.Name = "AzureDB-webrunner";
            mthread.Start();
        }


        protected override void Dispose(bool disposing)
        {
            if(disposing)
            {
                running = false;
                evt.Set();
            }
            mthread.Join();
            base.Dispose(disposing);
        }
        const int optimal_shard_size = 200; //Optimal shard size for a single Azure storage account (calculated through strange Azure voodoo)
        public override Task<ulong> GetShardCount()
        {
            TaskCompletionSource<ulong> retval = new TaskCompletionSource<ulong>();
            retval.SetResult(optimal_shard_size);
            return retval.Task;
        }


        protected override async Task RetrieveEntities(IEnumerable<ScalableEntity> entities, RetrieveCallback cb)
        {
            List<AzureOperationHandle> ops = null;
            
            ops = entities.Select(m => new AzureOperationHandle(m.SetPartition(optimal_shard_size), OpType.Retrieve) { callback = cb }).ToList();
            lock (evt)
            {
                foreach (var iable in ops)
                {
                    if (!pendingOperations.ContainsKey(iable.Entity.Partition))
                    {
                        pendingOperations.Add(iable.Entity.Partition, new List<AzureOperationHandle>());
                    }
                    pendingOperations[iable.Entity.Partition].Add(iable);
                }

                evt.Set();
            }
            await Task.WhenAll(ops.Select(m => m.Task.Task));
        }

        protected override Task RetrieveRange(byte[] start, byte[] end, RetrieveCallback cb)
        {
            AzureOperationHandle rangeRetrieve = new AzureOperationHandle(null, OpType.RangeRetrieve) { StartRange = start, EndRange = end, callback = cb };
            lock(evt)
            {
                if (!pendingOperations.ContainsKey(0))
                {
                    pendingOperations.Add(0, new List<AzureOperationHandle>());
                }
                pendingOperations[0].Add(rangeRetrieve);
                evt.Set();
            }
            return rangeRetrieve.Task.Task;

        }

        protected override async Task UpsertEntities(IEnumerable<ScalableEntity> entities)
        {
            var ops = entities.Select(m => new AzureOperationHandle(m.SetPartition(optimal_shard_size),OpType.Upsert)).ToList();
            lock(evt)
            {
                foreach(var iable in ops)
                {
                    if(!pendingOperations.ContainsKey(iable.Entity.Partition))
                    {
                        pendingOperations.Add(iable.Entity.Partition, new List<AzureOperationHandle>());
                    }
                    pendingOperations[iable.Entity.Partition].Add(iable);
                }

                evt.Set();
            }
            await Task.WhenAll(ops.Select(m => m.Task.Task));
        }


        protected override async Task DeleteEntities(IEnumerable<ScalableEntity> entities)
        {
            var ops = entities.Select(m => new AzureOperationHandle(m.SetPartition(optimal_shard_size), OpType.Delete)).ToList();
            lock (evt)
            {
                foreach (var iable in ops)
                {
                    if (!pendingOperations.ContainsKey(iable.Entity.Partition))
                    {
                        pendingOperations.Add(iable.Entity.Partition, new List<AzureOperationHandle>());
                    }
                    pendingOperations[iable.Entity.Partition].Add(iable);
                }

                evt.Set();
            }
            await Task.WhenAll(ops.Select(m => m.Task.Task));
        }

    }
}
