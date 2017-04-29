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
                        iable.Value = db[iable.Key];
                        retval.Add(iable);
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
                    db[iable.Key] = iable.Value;
                }
            }
            TaskCompletionSource<bool> s = new TaskCompletionSource<bool>();
            s.SetResult(true);
            return s.Task;
        }
    }
}
