using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB
{
    public class ScalableEntity
    {
        public byte[] Key;
        public byte[] Value;
        internal ulong Partition;
        public ScalableEntity(byte[] key, byte[] value)
        {
            Key = key;
            Value = value;
        }
        public ScalableEntity SetPartition(ulong partitionCount)
        {
            Partition = Key.Hash() % partitionCount;
            return this;
        }
    }

    public abstract class ScalableDb:IDisposable
    {
        //Database design for scalable architecture
        // Assumptions -- PartitionKey == server ID which == hash(key), RowKey == key
        // Secondary (optional) range indices -- PartitionKey == First N bits of key where N is the number of desired partitions

        public delegate bool RetrieveCallback(IEnumerable<ScalableEntity> entities);

        public ScalableDb()
        {
            
        }


        public async Task Retrieve(IEnumerable<byte[]> keys, RetrieveCallback cb)
        {
            var shardCount = await GetShardCount();
            var servers = await GetShardServers();

            if (servers == null)
            {
                await RetrieveEntities(keys.Select(m => new ScalableEntity(m, null).SetPartition(shardCount)), cb);
            }
            else
            {
                bool running = true;
                var shards = keys.Select(m => new ScalableEntity(m, null).SetPartition(shardCount)).ToLookup(m => m.Partition);
                List<Task> pending = new List<Task>();
                foreach (var shard in shards)
                {
                    pending.Add(servers[shard.Key].RetrieveEntities(shard,m=> {
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
                    }));
                }
                await Task.WhenAll(pending);
            }
        }


        /// <summary>
        /// Upserts entities into the database
        /// </summary>
        /// <param name="entities">The entities to upsert</param>
        public async Task Upsert(IEnumerable<ScalableEntity> entities)
        {
            var shardCount = await GetShardCount();
            var servers = await GetShardServers();
            
            if (servers == null)
            {
                await UpsertEntities(entities);
            }else
            {
                var shards = entities.Select(m => m.SetPartition(shardCount)).ToLookup(m => m.Partition);
                List<Task> pending = new List<Task>();
                foreach (var shard in shards)
                {
                    pending.Add(servers[shard.Key].UpsertEntities(shard));
                }
                await Task.WhenAll(pending);
            }
        }

        protected abstract Task RetrieveEntities(IEnumerable<ScalableEntity> entities, RetrieveCallback cb);
        protected abstract Task UpsertEntities(IEnumerable<ScalableEntity> entities);
        
        /// <summary>
        /// Override this method if GetShardServers returns null
        /// </summary>
        /// <returns></returns>
        public virtual async Task<ulong> GetShardCount()
        {
            return (ulong)(await GetShardServers()).Length;
        }

        /// <summary>
        /// Retrieves a list of shard servers
        /// </summary>
        public virtual async Task<ScalableDb[]> GetShardServers()
        {
            await Task.Yield();
            return null;
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    //Free managed state here
                }
                
                disposedValue = true;
            }
        }
        

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion
    }
}
