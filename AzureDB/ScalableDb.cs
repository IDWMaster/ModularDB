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
        
        public ScalableDb()
        {
            
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
