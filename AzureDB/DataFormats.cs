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
using System.Text;
using System.IO;
namespace AzureDB
{
    static class DataFormats
    {
        internal static unsafe byte[] EncodeDecimal(this decimal point)
        {
            byte* me = (byte*)&point;
            byte[] res = new byte[16];
            for (int i = 0; i < 16; i++)
            {
                res[16 - i - 1] = me[i];
            }
            return res;
        }
        internal static unsafe decimal DecodeDecimal(byte[] encoded)
        {
            decimal point;
            byte* dest = (byte*)&point;
            fixed (byte* me = encoded)
            {
                for (int i = 0; i < 16; i++)
                {
                    dest[16 - i - 1] = me[i];
                }
            }
            return point;
        }
        internal static unsafe long EncodeDouble(this double v)
        {
            return System.Net.IPAddress.HostToNetworkOrder(*(long*)&v);
        }
        internal static unsafe double DecodeDouble(this long er)
        {
            er = System.Net.IPAddress.NetworkToHostOrder(er);
            return *(double*)&er;
        }
        internal static int EncodeNumeric(this int eger)
        {
            return System.Net.IPAddress.HostToNetworkOrder(eger);
        }
        internal static long EncodeNumeric(this long eger)
        {
            return System.Net.IPAddress.HostToNetworkOrder(eger);
        }
        internal static int DecodeNumeric(this int eger)
        {
            return System.Net.IPAddress.NetworkToHostOrder(eger);
        }
        internal static long DecodeNumeric(this long eger)
        {
            return System.Net.IPAddress.NetworkToHostOrder(eger);
        }

        public static void WriteString(this BinaryWriter writer, string er)
        {
            byte[] data = Encoding.UTF8.GetBytes(er);
            writer.Write(data);
            writer.Write((byte)0);
        }
        public static string ReadNullTerminatedString(this BinaryReader mreader)
        {
            MemoryStream encoder = new MemoryStream();
            BinaryWriter mwriter = new BinaryWriter(encoder);
            int data = 0;
            while ((data = mreader.ReadByte()) != 0)
            {
                mwriter.Write((byte)data);
            }
            return Encoding.UTF8.GetString(encoder.ToArray());
        }

        internal static object Deserialize(byte[] array)
        {
            return Deserialize(new BinaryReader(new MemoryStream(array)));
        }
        internal static object Deserialize(BinaryReader mreader)
        {

            //bool,int,long,double,decimal,DateTime,string,byte[]

            switch (mreader.ReadByte())
            {
                case 0:
                    return mreader.ReadBoolean();
                case 1:
                    return mreader.ReadInt32().DecodeNumeric();
                case 2:
                    return mreader.ReadInt64().DecodeNumeric();
                case 3:
                    return DecodeDouble(mreader.ReadInt64());
                case 4:
                    return DecodeDecimal(mreader.ReadBytes(16));
                case 5:
                    return DateTime.FromBinary(mreader.ReadInt64().DecodeNumeric());
                case 6:
                    return mreader.ReadNullTerminatedString();
                case 7:
                    return mreader.ReadBytes(mreader.ReadInt32());
                case 8:
                    return new Guid(mreader.ReadBytes(16));
            }
            throw new InvalidDataException("Attempted to read an unsupported data type. The database client for this language either does not recognize the specified encoding; a non-standard encoding was used for this database, or this client is out-of-date and needs to be updated.");
        }
        internal static byte[] Serialize(this object obj)
        {
            MemoryStream mstream = new MemoryStream();
            BinaryWriter mwriter = new BinaryWriter(mstream);
            obj.Serialize(mwriter);
            return mstream.ToArray();
        }
        internal static void Serialize(this object obj, BinaryWriter mwriter)
        {
            //bool,int,long,double,decimal,DateTime,string,byte[]
            if (obj.GetType() == typeof(bool))
            {
                mwriter.Write((byte)0);
                mwriter.Write((bool)obj);
            }
            else
            {
                if (obj.GetType() == typeof(int))
                {
                    mwriter.Write((byte)1);
                    mwriter.Write(((int)obj).EncodeNumeric());
                }
                else
                {
                    if (obj.GetType() == typeof(long))
                    {
                        mwriter.Write((byte)2);
                        mwriter.Write(((long)obj).EncodeNumeric());
                    }
                    else
                    {
                        if (obj.GetType() == typeof(double))
                        {
                            mwriter.Write((byte)3);
                            mwriter.Write(((double)obj).EncodeDouble());
                        }
                        else
                        {
                            if (obj.GetType() == typeof(decimal))
                            {
                                mwriter.Write((byte)4);
                                mwriter.Write(((decimal)obj).EncodeDecimal());
                            }
                            else
                            {
                                if (obj.GetType() == typeof(DateTime))
                                {
                                    mwriter.Write((byte)5);
                                    mwriter.Write(((DateTime)obj).ToBinary().EncodeNumeric());
                                }
                                else
                                {
                                    if (obj.GetType() == typeof(string))
                                    {
                                        mwriter.Write((byte)6);
                                        mwriter.WriteString(obj as string);
                                    }
                                    else
                                    {
                                        if (obj.GetType() == typeof(byte[]))
                                        {
                                            mwriter.Write((byte)7);
                                            mwriter.Write((obj as byte[]).Length);
                                            mwriter.Write(obj as byte[]);
                                        }
                                        else
                                        {
                                            if (obj.GetType() == typeof(Guid))
                                            {
                                                mwriter.Write((byte)8);
                                                mwriter.Write(((Guid)obj).ToByteArray());
                                            }
                                            else
                                            {
                                                throw new NotImplementedException("Unsupported data type: " + obj.GetType() + ". Please contact IDWDB support if you would like support for this data type.");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
