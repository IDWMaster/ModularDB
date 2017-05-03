using Microsoft.VisualStudio.TestTools.UnitTesting;
using AzureDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB.Tests
{
    class SampleEntity
    {
        public byte[] Key { get; set; }
        public byte[] Value { get; set; }
    }
    [TestClass()]
    public class TableTests
    {
        [TestMethod()]
        public void UpsertTest()
        {
            Random mrand = new Random();
            using(TableDb db = new TableDb(new MemoryDb()))
            {
                SampleEntity[] entities = new SampleEntity[5000];
                for(int i = 0;i<entities.Length;i++)
                {
                    byte[] key = new byte[150];
                    byte[] value = new byte[1024];
                    mrand.NextBytes(value);
                    mrand.NextBytes(key);
                    entities[i] = new SampleEntity() { Key = key, Value = value };
                }
                db["test"].Upsert(entities).Wait();
                List<SampleEntity> values = new List<SampleEntity>();
                db["test"].Retrieve<SampleEntity>(entities.Select(m => m.Key), rows => {
                    lock(values)
                    {
                        values.AddRange(rows);
                    }
                    return true;
                }).Wait();
                Assert.AreEqual(entities.Length, values.Count);
            }
        }
    }
}