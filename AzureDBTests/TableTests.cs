using Microsoft.VisualStudio.TestTools.UnitTesting;
using AzureDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB.Tests
{
    class RangedEntity
    {
        public int Key { get; set; }
        public byte[] Value { get; set; }

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
                for(int i = 0;i<entities.Length;i++)
                {
                    CollectionAssert.AreEqual(entities[i].Value, values[i].Value);
                }
            }
        }
            [TestMethod()]
            public void RangedUpsertTest()
            {
                Random mrand = new Random();
                using (RangedTableDb db = new RangedTableDb(new MemoryDb()))
                {
                    RangedEntity[] entities = new RangedEntity[5000];
                    for (int i = 0; i < entities.Length; i++)
                    {
                        byte[] value = new byte[1024];
                        mrand.NextBytes(value);
                        entities[i] = new RangedEntity() { Key = i, Value = value };
                    }
                    db["test"].Upsert(entities).Wait();
                    List<RangedEntity> values = new List<RangedEntity>();
                    db["test"].Retrieve<RangedEntity>(0, entities.Length - 1, rows =>
                    {
                        lock (values)
                        {
                            values.AddRange(rows);
                        }
                        return true;
                    }).Wait();
                    Assert.AreEqual(entities.Length-2, values.Count);
                    for (int i = 0; i < entities.Length-2; i++)
                    {
                        CollectionAssert.AreEqual(entities[i+1].Value, values[i].Value);
                    }

                    List<TableRow> results = new List<TableRow>();
                    db["test"].Retrieve(50, 100, m =>
                    {
                        results.AddRange(m);
                        return true;
                    }).Wait();
                    Assert.AreEqual(results.Count, 49);

                }
            }
        }

    }
}