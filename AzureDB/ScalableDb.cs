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
using System.IO;
using System.ComponentModel.DataAnnotations;
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
        public delegate bool TypedRetrieveCallback<T>(IEnumerable<T> entities);
        public ScalableDb()
        {
            
        }
        public async Task Retrieve<T>(IEnumerable<object> keys, TypedRetrieveCallback<T> callback) where T:class, new()
        {
            var keyFields = typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance).Where(m => m.CustomAttributes.Where(a => a.AttributeType == typeof(KeyAttribute)).Any() || m.Name == "Key");
            if(!keyFields.Any())
            {
                throw new InvalidCastException("Type "+typeof(T).Name+" does not have a Key property. Please declare a Key property.");
            }
            var keyField = keyFields.First();
            await Retrieve(keys.Select(m => m.Serialize()), elems => {
                return callback(elems.Select(m=> {
                    T retval = new T();
                    
                    BinaryReader mreader = new BinaryReader(new MemoryStream(m.Value));
                    object key = keyField.PropertyType == typeof(byte[]) ? m.Key : DataFormats.Deserialize(m.Key);
                    keyField.SetValue(retval, key);
                    while(mreader.BaseStream.Position != mreader.BaseStream.Length)
                    {
                        string props = mreader.ReadNullTerminatedString();
                        var prop = typeof(T).GetProperty(props, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                        if(prop != null)
                        {
                            prop.SetValue(retval, DataFormats.Deserialize(mreader));
                        }
                    }
                    return retval;
                }));
            });
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

        public async Task Upsert<T>(IEnumerable<T> rows)
        {
            var keyFields = typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance).Where(m => m.CustomAttributes.Where(a => a.AttributeType == typeof(KeyAttribute)).Any() || m.Name == "Key");
            if (!keyFields.Any())
            {
                throw new InvalidCastException("Type " + typeof(T).Name + " does not have a Key property. Please declare a Key property.");
            }
            var keyField = keyFields.First();
            await Upsert(rows.Select(m => {

                byte[] key = keyField.PropertyType == typeof(byte[]) ? keyField.GetValue(m) as byte[] : keyField.GetValue(m).Serialize();
                MemoryStream mstream = new MemoryStream();
                BinaryWriter mwriter = new BinaryWriter(mstream);
                foreach(var iable in typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance))
                {
                    mwriter.WriteString(iable.Name);
                    iable.GetValue(m).Serialize(mwriter);
                }
                return new ScalableEntity(key, mstream.ToArray());
            }));
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
