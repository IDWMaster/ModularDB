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
    public class ByteComparer : IEqualityComparer<byte[]>, IComparer<byte[]>
    {
       public static ByteComparer instance = new ByteComparer();

        public int Compare(byte[] x, byte[] y)
        {
            for(int i = 0;i<x.Length;i++)
            {
                if(x[i]<y[i])
                {
                    return -1;
                }
                if(x[i]>y[i])
                {
                    return 1;
                }
            }
            return 0;
        }

        public bool Equals(byte[] x, byte[] y)
        {
            int len = x.Length;
            if(x.Length != y.Length)
            {
                return false;
            }
            for(int i = 0;i<len;i++)
            {
                if(x[i] != y[i])
                {
                    return false;
                }
            }
            return true;
        }

        public int GetHashCode(byte[] obj)
        {
            return (int)obj.Hash();
        }
    }
    public class EntityComparer : IEqualityComparer<ScalableEntity>, IComparer<ScalableEntity>, System.Collections.IComparer
    {
        public static EntityComparer instance = new EntityComparer();

        public int Compare(ScalableEntity x, ScalableEntity y)
        {
            return ByteComparer.instance.Compare(x.Key, y.Key);
        }

        public int Compare(object x, object y)
        {
            if(x == y)
            {
                return 0;
            }
            return Compare(x as ScalableEntity, y as ScalableEntity);
        }

        public bool Equals(ScalableEntity x, ScalableEntity y)
        {
           return ByteComparer.instance.Equals(x.Key, y.Key);
        }

        public int GetHashCode(ScalableEntity obj)
        {
            return (int)obj.Key.Hash();
        }
    }

    public static class Algorithms
    {
        public static ulong LinearHash(this byte[] array)
        {
            ulong forme = 0;
            int count = array.Length >= sizeof(ulong) ? sizeof(ulong) : array.Length;
            for(int i = 0;i<count;i++)
            {
                forme |= (ulong)(array[i] << ((sizeof(ulong)-i-1)*8));
            }
            
            return forme;
        }
        public static byte[] PadTo(this byte[] array,int numBytes)
        {
            byte[] retval = new byte[numBytes];
            Buffer.BlockCopy(array, 0, retval, 0, array.Length);
            return retval;
        }
        public unsafe static ulong Hash(byte* ptr, int length)
        {
            ulong hash = 14695981039346656037;
            for (int i = 0; i < length; i++)
            {
                hash = (hash ^ ptr[i]) * 1099511628211;
            }
            return hash;
        }
        /// <summary>
        /// Generates a statistically uniform hash of a byte array. The resulting hash is ideal for point queries (partitioning), but will not work for range queries.
        /// </summary>
        /// <returns></returns>
        public static unsafe ulong Hash(this byte[] data)
        {
            fixed(byte* ptr = data)
            {
                return Hash(ptr, data.Length);
            }
        }
    }
}
