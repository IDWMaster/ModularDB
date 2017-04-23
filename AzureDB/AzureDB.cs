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
        Upsert
    }
    class AzureOperationHandle
    {
        public TaskCompletionSource<ScalableEntity> Task;
        public ScalableEntity Entity;
        public OpType Type;
        public AzureOperationHandle(ScalableEntity entity, OpType type)
        {
            Entity = entity;
            Task = new TaskCompletionSource<ScalableEntity>();
            Type = type;
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
                        TableBatchOperation batch = new TableBatchOperation();
                        foreach(var op in shard.Value.Where(m=>m.Type == OpType.Upsert))
                        {
                            
                            batch.Add(TableOperation.InsertOrReplace(new AzureEntity() { PartitionKey = op.Entity.Partition.ToString(), RowKey = Uri.EscapeDataString(Convert.ToBase64String(op.Entity.Key)), Value = op.Entity.Value }));
                            if (batch.Count == 100)
                            {
                                runningTasks.Add(table.ExecuteBatchAsync(batch));
                                batch = new TableBatchOperation();
                            }
                        }
                        if(batch.Any())
                        {
                            runningTasks.Add(table.ExecuteBatchAsync(batch));
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
