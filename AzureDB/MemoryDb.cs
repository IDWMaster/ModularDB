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

namespace AzureDB
{
    

    /// <summary>
    /// A database stored in local RAM
    /// </summary>
    public class MemoryDb:ScalableDb
    {
        Dictionary<byte[], byte[]> db;
        byte[][] index = new byte[1][];
        int indexLen;
        public MemoryDb()
        {
            db = new Dictionary<byte[], byte[]>(new ByteComparer());
        }
        public override Task<ulong> GetShardCount()
        {
            TaskCompletionSource<ulong> src = new TaskCompletionSource<ulong>();
            src.SetResult(1);
            return src.Task;
        }

        

        protected override Task RetrieveRange(byte[] start, byte[] end, RetrieveCallback cb)
        {
            TaskCompletionSource<bool> tsktsktsktsk = new TaskCompletionSource<bool>();
            List<ScalableEntity> entities = new List<ScalableEntity>();

            lock (db)
            {
                int startIdx = 0;
                if (start != null)
                {
                    int found = Array.BinarySearch(index, start, new ByteComparer());
                    if (found < 0)
                    {
                        startIdx = ~found;
                    }
                    else
                    {
                        startIdx = found + 1; //exclusive search
                    }
                }
                for (int i = startIdx; i < indexLen; i++)
                {
                    byte[] key = index[i];
                    if (end != null)
                    {
                        if (ByteComparer.instance.Compare(key, end) >= 0)
                        {
                            break;
                        }
                    }
                    entities.Add(new ScalableEntity(key, db[key]));
                }
            }
            cb(entities);
            tsktsktsktsk.SetResult(true);
            return tsktsktsktsk.Task;
        }

        protected override Task RetrieveEntities(IEnumerable<ScalableEntity> entities, RetrieveCallback cb)
        {
            TaskCompletionSource<bool> src = new TaskCompletionSource<bool>();

            List<ScalableEntity> retval = new List<ScalableEntity>();
            lock (db)
            {
                foreach (var iable in entities)
                {
                    if(db.ContainsKey(iable.Key))
                    {
                        var found = db[iable.Key];
                        byte[] newkey = new byte[iable.Key.Length];
                        byte[] newvalue = new byte[found.Length];
                        Buffer.BlockCopy(iable.Key, 0, newkey, 0, newkey.Length);
                        Buffer.BlockCopy(found, 0, newvalue, 0, newvalue.Length);
                        retval.Add(new ScalableEntity(newkey, newvalue));
                    }
                }
            }
            if(retval.Any())
            {
                cb(retval);
            }
            src.SetResult(true);
            return src.Task;
        }

        protected override Task UpsertEntities(IEnumerable<ScalableEntity> entities)
        {
            lock (db)
            {
                foreach (var iable in entities)
                {
                    if(iable.Value == null)
                    {
                        throw new NullReferenceException("Value cannot be null.");
                    }
                    if(index.Length == indexLen)
                    {
                        byte[][] newIndex = new byte[index.Length * 2][];
                        Array.Copy(index, newIndex, index.Length);
                        index = newIndex;
                    }
                    int idx = Array.BinarySearch(index,0,indexLen, iable.Key,ByteComparer.instance);
                    if(idx<0)
                    {
                        idx = ~idx;
                    }
                    Array.ConstrainedCopy(index, idx, index, idx + 1, indexLen-idx);
                    index[idx] = iable.Key;
                    indexLen++;

                    db[iable.Key] = iable.Value;
                }
            }
            TaskCompletionSource<bool> s = new TaskCompletionSource<bool>();
            s.SetResult(true);
            return s.Task;
        }
    }
}
