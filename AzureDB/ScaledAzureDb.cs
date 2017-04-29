using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB
{
    public class ScaledAzureDb : ScalableDb
    {
        AzureDatabase[] databases;
        public ScaledAzureDb(AzureDatabase[] databases)
        {
            this.databases = databases;
        }
        public override Task<ScalableDb[]> GetShardServers()
        {
            TaskCompletionSource<ScalableDb[]> retval = new TaskCompletionSource<ScalableDb[]>();
            retval.SetResult(databases);
            return retval.Task;
        }
        protected override Task RetrieveEntities(IEnumerable<ScalableEntity> entities, RetrieveCallback cb)
        {
            throw new NotImplementedException();
        }
        protected override Task UpsertEntities(IEnumerable<ScalableEntity> entities)
        {
            throw new NotImplementedException();
        }
    }
}
