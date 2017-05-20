using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureDB
{
    //TODO: Transactions
    /**
     * Begin -- Start writing log file, with key being current DateTime+Guid. Write pointers to redirect I/O to commit logfile (by transaction ID).
     * Commit -- Add commit flag to transaction; causing the I/O to be redirected into the logfile.
     * Finalize -- Update entries on disk. Delete pointers, followed by logfile.
     * */
     

    class LogEntry
    {
        // TransactionID+Record pointer
        public byte[] Key { get; set; }
        // 0 == Upsert
        public int Opcode { get; set; }

    }
    public class Transaction
    {
        TransactedDb db;
        internal Transaction(TransactedDb db)
        {
            this.db = db;
        }

    }
    public class TransactedDb
    {
        internal TableDb db;
        public TransactedDb(TableDb db)
        {
            this.db = db;
        }
        
    }
}
