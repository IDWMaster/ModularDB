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
    public class ScaledAzureDb : ScalableDb
    {
        AzureDatabase[] databases;
        public ScaledAzureDb(AzureDatabase[] databases)
        {
            this.databases = databases;
        }
        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);
            if (disposing)
            {
                foreach(var iable in databases)
                {
                    iable.Dispose();
                }
            }
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
