using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB
{
    class ByteComparer : IEqualityComparer<byte[]>
    {
        public bool Equals(byte[] x, byte[] y)
        {
            return x.SequenceEqual(y);
        }

        public int GetHashCode(byte[] obj)
        {
            return (int)obj.Hash();
        }
    }

    /// <summary>
    /// A database stored in local RAM
    /// </summary>
    public class MemoryDb:ScalableDb
    {
        Dictionary<byte[], byte[]> db;
        public MemoryDb()
        {
            db = new Dictionary<byte[], byte[]>(new ByteComparer());
        }
        public override Task<ulong> GetShardCount()
        {
            TaskCompletionSource<ulong> src = new TaskCompletionSource<ulong>();
            src.SetResult(ulong.MaxValue);
            return src.Task;
        }

        protected override Task UpsertEntities(IEnumerable<ScalableEntity> entities)
        {
            lock (db)
            {
                foreach (var iable in entities)
                {
                    db[iable.Key] = iable.Value;
                }
            }
            TaskCompletionSource<bool> s = new TaskCompletionSource<bool>();
            s.SetResult(true);
            return s.Task;
        }
    }
}
