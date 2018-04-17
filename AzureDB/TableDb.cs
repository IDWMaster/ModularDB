using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB
{

    

    public class TableRow:IEnumerable<KeyValuePair<string,object>>
    {
        Dictionary<string, object> keys = new Dictionary<string, object>();
        public byte[] Key { get; internal set; }
        public object this[string key]
        {
            get
            {
                return keys.ContainsKey(key) ? keys[key] : null;
            }
            set
            {
                keys[key] = value;
            }
        }

        public static TableRow From(object obj)
        {
            var keyFields = obj.GetType().GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance).Where(m => m.CustomAttributes.Where(a => a.AttributeType == typeof(KeyAttribute)).Any() || m.Name == "Key");
            if (!keyFields.Any())
            {
                throw new InvalidCastException("Type " + obj.GetType().Name + " does not have a Key property. Please declare a Key property.");
            }
            TableRow retval = new TableRow();
            var keyField = keyFields.First();
            retval.Key = keyField.PropertyType == typeof(byte[]) ? keyField.GetValue(obj) as byte[] : keyField.GetValue(obj).Serialize();
            foreach(var iable in obj.GetType().GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance))
            {
                retval.keys[iable.Name] = iable.GetValue(obj);
            }
            return retval;
        }
        internal bool UseLinearHash = false;
        public T As<T>() where T:class,new()
        {
            T retval = new T();
            var keyFields = typeof(T).GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance).Where(m => m.CustomAttributes.Where(a => a.AttributeType == typeof(KeyAttribute)).Any() || m.Name == "Key");
            if (!keyFields.Any())
            {
                throw new InvalidCastException("Type " + typeof(T).Name + " does not have a Key property. Please declare a Key property.");
            }
            var keyField = keyFields.First();
            keyField.SetValue(retval, keyField.PropertyType == typeof(byte[]) ? Key : DataFormats.Deserialize(Key));
            foreach (var iable in keys)
            {
                var prop = typeof(T).GetProperty(iable.Key, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                prop?.SetValue(retval, iable.Value);
            }
            return retval;
        }

        public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
        {
            return keys.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return keys.GetEnumerator();
        }
    }


    public class Table
    {
        public ScalableDb db;
        TableDb tdb;
        byte[] tableName;
        string name;

        internal bool UseLinearHash = false;
        public string Name
        {
            get
            {
                return name;
            }
        }
        public delegate bool TypedRetrieveCallback<T>(IEnumerable<T> entities);
        const char nextChar = (char)('_' + 1);
        internal Table(TableDb tdb,ScalableDb db, string table, bool useLinearHash = false)
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
                TableRow row = new TableRow() { UseLinearHash = true, Key = tableName };
                await tdb["__tables"].Upsert(row);
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


        

        public Task Retrieve(IEnumerable<object> keys, TypedRetrieveCallback<TableRow> callback)
        {
            //Compute direct keys
            List<byte[]> directKeys = keys.Select(m => m.GetType() == typeof(byte[]) ? m as byte[] : m.Serialize()).Select(m =>
            {
                return GenerateKey(m);
            }).ToList();
            return RetrieveDirect(directKeys,callback);
        }

        private byte[] GenerateKey(byte[] m)
        {
            byte[] me = new byte[tableName.Length + m.Length];
            Buffer.BlockCopy(tableName, 0, me, 0, tableName.Length);
            Buffer.BlockCopy(m, 0, me, tableName.Length, m.Length);
            return me;
        }

        async Task RetrieveDirect(IEnumerable<byte[]> keys, TypedRetrieveCallback<TableRow> callback)
        {
            await db.Retrieve(keys, elems => {
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


        internal Task RangeRetrieve(byte[] start, byte[] end, TypedRetrieveCallback<TableRow> callback)
        {
            return RetrieveDirect(GenerateKey(start), GenerateKey(end), callback);
        }

        async Task RetrieveDirect(byte[] start, byte[] end, TypedRetrieveCallback<TableRow> callback)
        {



            await db.Retrieve(start,end, elems => {
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
        

        public Task Retrieve<T>(IEnumerable<object> keys, TypedRetrieveCallback<T> callback) where T : class, new()
        {
            return Retrieve(keys, rows=>callback(rows.Select(m=>m.As<T>())));
        }


        public Task Retrieve(object start, object end, TypedRetrieveCallback<TableRow> callback)
        {
            byte[] _start = start.GetType() == typeof(byte[]) ? start as byte[] : start.Serialize();
            byte[] _end = end.GetType() == typeof(byte[]) ? end as byte[] : end.Serialize();
            return RangeRetrieve(_start, _end, callback);
        }
        public Task Retrieve<T>(object start, object end, TypedRetrieveCallback<T> callback) where T : class, new()
        {
            return Retrieve(start,end, rows => callback(rows.Select(m=>m.As<T>())));
        }

        public Task Delete(IEnumerable<object> keys)
        {
            return Delete(keys.Select(m => m.GetType() == typeof(byte[]) ? m as byte[] : m.Serialize()));
        }

        
        public Task Upsert<T>(params T[] rows)
        {
            return Upsert(rows as IEnumerable<T>);
        }
        
        public Task Upsert<T>(IEnumerable<T> rows)
        {
            return Upsert(rows.Select(row => TableRow.From(row)));
        }

        
        public async Task Upsert(IEnumerable<TableRow> rows)
        {

            await db.Upsert(rows.Select(m => {
                byte[] key = m.Key;
                MemoryStream mstream = new MemoryStream();
                BinaryWriter mwriter = new BinaryWriter(mstream);
                foreach (var iable in m)
                {
                    mwriter.WriteString(iable.Key);
                    iable.Value.Serialize(mwriter);
                }
                byte[] me = new byte[key.Length + tableName.Length];
                Buffer.BlockCopy(tableName, 0, me, 0, tableName.Length);
                Buffer.BlockCopy(key, 0, me, tableName.Length, key.Length);
                if(this.UseLinearHash)
                {
                    m.UseLinearHash = true;
                }
                return new ScalableEntity(me, mstream.ToArray()) { UseLinearHash = m.UseLinearHash };
            }));
        }

        public async Task Delete(IEnumerable<byte[]> rows)
        {

            await db.Delete(rows.Select(key => {
                byte[] me = new byte[key.Length + tableName.Length];
                Buffer.BlockCopy(tableName, 0, me, 0, tableName.Length);
                Buffer.BlockCopy(key, 0, me, tableName.Length, key.Length);
                return me;
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

        public virtual Table this[string name]
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

    /// <summary>
    /// Table-driven database class
    /// </summary>
    public class RangedTableDb : TableDb
    {
        ScalableDb db;
        public RangedTableDb(ScalableDb db):base(db)
        {
            this.db = db;
        }

        public override Table this[string name]
        {
            get
            {
                return new Table(this, db, name, true);
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
