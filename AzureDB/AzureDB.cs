using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Auth;
using System.Threading;
namespace AzureDB
{
    enum OpType
    {
        Upsert, Retrieve, Nop
    }
    class AzureOperationHandle
    {
        public TaskCompletionSource<ScalableEntity> Task;
        public ScalableEntity Entity;
        public OpType Type;
        public ScalableDb.RetrieveCallback callback;
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
        CloudTableClient client;
        CloudTable table;
        Dictionary<ulong, List<AzureOperationHandle>> pendingOperations = new Dictionary<ulong, List<AzureOperationHandle>>();
        ManualResetEvent evt = new ManualResetEvent(false);
        bool running = true;
        public AzureDatabase(string storageAccountString, string tableName)
        {
            CloudStorageAccount account = CloudStorageAccount.Parse(storageAccountString);
            client = account.CreateCloudTableClient();
            client.DefaultRequestOptions.PayloadFormat = TablePayloadFormat.JsonNoMetadata;
            table = client.GetTableReference(tableName);
            System.Threading.Thread mthread = new Thread(async delegate () {
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
                        Dictionary<ScalableEntity,List<AzureOperationHandle>> retrieves = new Dictionary<ScalableEntity, List<AzureOperationHandle>>(EntityComparer.instance);
                        foreach(var op in shard.Value.Where(m=>m.Type == OpType.Upsert))
                        {
                            
                            upserts.Add(TableOperation.InsertOrReplace(new AzureEntity() { PartitionKey = op.Entity.Partition.ToString(), RowKey = Uri.EscapeDataString(Convert.ToBase64String(op.Entity.Key)), Value = op.Entity.Value }));
                            if (upserts.Count == 100)
                            {
                                runningTasks.Add(table.ExecuteBatchAsync(upserts));
                                upserts = new TableBatchOperation();
                            }
                        }
                        Func<Dictionary<ScalableEntity,List<AzureOperationHandle>>,TableContinuationToken,TableQuery<AzureEntity>, Task> runSegmentedQuery = null;
                        runSegmentedQuery = async (tableops,token, compiledQuery) => {
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
                                compiledQuery = new TableQuery<AzureEntity>().Where(query);
                            }
                            var segment = await table.ExecuteQuerySegmentedAsync(compiledQuery, token);
                            token = segment.ContinuationToken;
                            List<AzureOperationHandle> finished = new List<AzureOperationHandle>();
                            foreach (var iable in segment)
                            {
                                var ent = new ScalableEntity(Convert.FromBase64String(Uri.UnescapeDataString(iable.RowKey)),null);
                                finished.AddRange(tableops[ent].Select(m=>m.SetValue(ent.Value)));
                            }
                            //Combine callbacks for finished queries
                            var callbacks = finished.ToLookup(m => m.callback);
                            foreach(var iable in callbacks)
                            {
                                if(!iable.Key(iable.Select(m=>m.Entity)))
                                {
                                    iable.AsParallel().ForAll(m => m.Type = OpType.Nop);
                                    compiledQuery = null; //Re-compile query
                                }
                            }
                            if (token != null)
                            {
                                await runSegmentedQuery(tableops, token, compiledQuery);
                            }
                        };
                        foreach (var op in shard.Value.Where(m=>m.Type == OpType.Retrieve))
                        {
                            if(!retrieves.ContainsKey(op.Entity))
                            {
                                retrieves.Add(op.Entity, new List<AzureOperationHandle>());
                            }
                            retrieves[op.Entity].Add(op);
                            if (retrieves.Count == 100)
                            {
                                runningTasks.Add(runSegmentedQuery(retrieves,null,null));
                                retrieves = new Dictionary<ScalableEntity, List<AzureOperationHandle>>(EntityComparer.instance);
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
                        if(retrieves.Any())
                        {  
                            runningTasks.Add(runSegmentedQuery(retrieves,null,null));
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
            bool running = true;
            List<AzureOperationHandle> ops = null;
            ScalableDb.RetrieveCallback localCallback = m => {
                if(!running)
                {        
                    return false;
                }
                if (!cb(m))
                {
                    running = false;
                    return false;
                }
                return true;
            };
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
    }
}
