using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB
{
    class TableMetadata
    {
        public string Key { get; set; }
    }

    public class TableRow
    {
        Dictionary<string, object> keys = new Dictionary<string, object>();
        public byte[] Key { get; internal set; }
        public object this[string key]
        {
            get
            {
                return keys.ContainsKey(key) ? keys[key] : null;
            } internal set
            {
                keys[key] = value;
            }
        }
    }


    public class Table
    {
        public ScalableDb db;
        TableDb tdb;
        byte[] tableName;
        string name;
        public string Name
        {
            get
            {
                return name;
            }
        }
        public delegate bool TypedRetrieveCallback<T>(IEnumerable<T> entities);
        const char nextChar = (char)('_' + 1);
        internal Table(TableDb tdb,ScalableDb db, string table)
        {
            tableName = Encoding.UTF8.GetBytes(table+"_");
            this.db = db;
            name = table;
            this.tdb = tdb;
            switch(table)
            {
                case "__tables":
                    initialized = true;
                    break;
                default:
                    break;
            }
        }

        bool initialized = false;

        async Task Initialize()
        {
            if(!initialized)
            {
                await tdb["__tables"].Upsert(new { Key = tableName });
                initialized = true;
            }
        }

        public async Task<IEnumerable<T>> RetrieveMany<T>(params object[] keys) where T:class, new()
        {
            List<T> retval = new List<T>();
            await Retrieve<T>(keys, rows => {
                lock (retval)
                {
                    retval.AddRange(rows);
                }
                return true;
            });
            return retval;
        }

        public async Task<IEnumerable<TableRow>> RetrieveMany(params object[] keys)
        {
            List<TableRow> retval = new List<TableRow>();
            await Retrieve(keys, rows => {
                lock (retval)
                {
                    retval.AddRange(rows);
                }
                return true;
            });
            return retval;
        }

        public async Task<TableRow> RetrieveOne(object key)
        {
            return (await RetrieveMany(key)).FirstOrDefault();
        }

        public async Task<T> RetrieveOne<T>(object key) where T:class, new()
        {
            return (await RetrieveMany<T>(key)).FirstOrDefault();
        }


        public async Task Retrieve(IEnumerable<object> keys, TypedRetrieveCallback<TableRow> callback)
        {
            await db.Retrieve(keys.Select(m => {

                byte[] data = m.GetType() == typeof(byte[]) ? m as byte[] : m.Serialize();
                byte[] newdata = new byte[tableName.Length + data.Length];
                Buffer.BlockCopy(tableName, 0, newdata, 0, tableName.Length);
                Buffer.BlockCopy(data, 0, newdata, tableName.Length, data.Length);
                return newdata;
            }), elems => {
                return callback(elems.Select(m => {
                    byte[] newkey = new byte[m.Key.Length - tableName.Length];
                    Buffer.BlockCopy(m.Key, tableName.Length, newkey, 0, newkey.Length);
                    m.Key = newkey;

                    TableRow retval = new TableRow();
                    BinaryReader mreader = new BinaryReader(new MemoryStream(m.Value));
                    retval.Key = m.Key;
                    while (mreader.BaseStream.Position != mreader.BaseStream.Length)
                    {
                        retval[mreader.ReadNullTerminatedString()] = DataFormats.Deserialize(mreader);
                    }
                    return retval;
                }));
            });
        }


        public async Task Retrieve<T>(IEnumerable<object> keys, TypedRetrieveCallback<T> callback) where T : class, new()
        {
            var keyFields = typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance).Where(m => m.CustomAttributes.Where(a => a.AttributeType == typeof(KeyAttribute)).Any() || m.Name == "Key");
            if (!keyFields.Any())
            {
                throw new InvalidCastException("Type " + typeof(T).Name + " does not have a Key property. Please declare a Key property.");
            }
            var keyField = keyFields.First();
            await db.Retrieve(keys.Select(m => {

                byte[] data = m.GetType() == typeof(byte[]) ? m as byte[] : m.Serialize();
                byte[] newdata = new byte[tableName.Length + data.Length];
                Buffer.BlockCopy(tableName, 0, newdata, 0, tableName.Length);
                Buffer.BlockCopy(data, 0, newdata, tableName.Length, data.Length);
                return newdata;
            }), elems => {
                return callback(elems.Select(m => {
                    byte[] newkey = new byte[m.Key.Length - tableName.Length];
                    Buffer.BlockCopy(m.Key, tableName.Length, newkey, 0, newkey.Length);
                    m.Key = newkey;

                    T retval = new T();
                    BinaryReader mreader = new BinaryReader(new MemoryStream(m.Value));
                    object key = keyField.PropertyType == typeof(byte[]) ? m.Key : DataFormats.Deserialize(m.Key);
                    keyField.SetValue(retval, key);
                    while (mreader.BaseStream.Position != mreader.BaseStream.Length)
                    {
                        string props = mreader.ReadNullTerminatedString();
                        var prop = typeof(T).GetProperty(props, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                        if (prop != null)
                        {
                            prop.SetValue(retval, DataFormats.Deserialize(mreader));
                        }
                    }
                    return retval;
                }));
            });
        }
        

        public Task Upsert<T>(params T[] rows)
        {
            return Upsert(rows as IEnumerable<T>);
        }

        public async Task Upsert<T>(IEnumerable<T> rows)
        {
            var keyFields = typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance).Where(m => m.CustomAttributes.Where(a => a.AttributeType == typeof(KeyAttribute)).Any() || m.Name == "Key");
            if (!keyFields.Any())
            {
                throw new InvalidCastException("Type " + typeof(T).Name + " does not have a Key property. Please declare a Key property.");
            }
            var keyField = keyFields.First();
            await db.Upsert(rows.Select(m => {

                byte[] key = keyField.PropertyType == typeof(byte[]) ? keyField.GetValue(m) as byte[] : keyField.GetValue(m).Serialize();
                MemoryStream mstream = new MemoryStream();
                BinaryWriter mwriter = new BinaryWriter(mstream);
                foreach (var iable in typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance))
                {
                    mwriter.WriteString(iable.Name);
                    iable.GetValue(m).Serialize(mwriter);
                }
                byte[] me = new byte[key.Length + tableName.Length];
                Buffer.BlockCopy(tableName, 0, me, 0, tableName.Length);
                Buffer.BlockCopy(key, 0, me, tableName.Length, key.Length);

                return new ScalableEntity(me, mstream.ToArray());
            }));
        }
    }
    /// <summary>
    /// Table-driven database class
    /// </summary>
    public class TableDb : IDisposable
    {
        ScalableDb db;
        public TableDb(ScalableDb db)
        {
            this.db = db;
        }

        public Table this[string name]
        {
            get
            {
                return new Table(this,db, name);
            }
        }
        

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    db.Dispose();
                }
                
                disposedValue = true;
            }
        }
        
        public void Dispose()
        {
            Dispose(true);
        }
        #endregion

    }
}
