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
using System.Net;
using System.Net.Sockets;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.ComponentModel.DataAnnotations;
namespace AzureDB
{
    public class ScalableEntity
    {
        public byte[] Key;
        public byte[] Value;
        internal ulong Partition;
        public bool UseLinearHash = false;
        public ScalableEntity(byte[] key, byte[] value)
        {
            Key = key;
            Value = value;
        }
        public ScalableEntity SetPartition(ulong partitionCount)
        {
            if (UseLinearHash)
            {
                Partition = Key.LinearHash() % partitionCount;
            }
            else
            {
                Partition = Key.Hash() % partitionCount;
            }
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
        

        protected abstract Task DeleteEntities(IEnumerable<ScalableEntity> entities);

        protected abstract Task RetrieveRange(byte[] start, byte[] end, RetrieveCallback cb);

        public async Task Retrieve(byte[] start, byte[] end, RetrieveCallback cb)
        {
            var shardCount = await GetShardCount();
            var servers = await GetShardServers();

            if (servers == null)
            {
                await RetrieveRange(start, end, cb);
            }
            else
            {
                bool running = true;
                int a = 0;
                int b = (int)shardCount;
                if(start != null)
                {
                    a = (int)(start.LinearHash() % shardCount);
                }
                if(end != null)
                {
                    b = (int)(end.LinearHash() % shardCount)+1;
                }

                List<Task> pending = new List<Task>();
                for (int i = a;i<b;i++)
                {
                    pending.Add(servers[i].RetrieveRange(start,end, m => {
                        if (!running)
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


        /// <summary>
        /// Deletes entities from the database
        /// </summary>
        /// <param name="entities">The entities to upsert</param>
        public async Task Delete(IEnumerable<byte[]> keys)
        {
            var shardCount = await GetShardCount();
            var servers = await GetShardServers();

            if (servers == null)
            {
                await DeleteEntities(keys.Select(m => new ScalableEntity(m, null).SetPartition(shardCount)));
            }
            else
            {
                bool running = true;
                var shards = keys.Select(m => new ScalableEntity(m, null).SetPartition(shardCount)).ToLookup(m => m.Partition);
                List<Task> pending = new List<Task>();
                foreach (var shard in shards)
                {
                    pending.Add(servers[shard.Key].DeleteEntities(shard));
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
