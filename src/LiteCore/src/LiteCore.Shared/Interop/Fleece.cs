﻿//
// Fleece.cs
//
// Copyright (c) 2016 Couchbase, Inc All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

using Couchbase.Lite;

using LiteCore.Util;

namespace LiteCore.Interop
{
    using Couchbase.Lite.Interop;

    internal unsafe interface IFLEncodable
    {
        void FLEncode(FLEncoder* enc);
    }

    internal unsafe partial struct FLSlice
    {
        public static readonly FLSlice Null = new FLSlice(null, 0);

        private static readonly ConcurrentDictionary<string, FLSlice> _Constants =
            new ConcurrentDictionary<string, FLSlice>();

        public FLSlice(void* buf, ulong size)
        {
            this.buf = buf;
            _size = (UIntPtr)size;
        }

        public static FLSlice Constant(string input)
        {
            // Warning: This creates unmanaged memory that is intended never to be freed
            // You should only use it with constant strings
            return _Constants.GetOrAdd(input, Allocate);
        }

        public static FLSlice Allocate(string input)
        {
            var bytes = Encoding.UTF8.GetBytes(input);
            var intPtr = Marshal.AllocHGlobal(bytes.Length);
            Marshal.Copy(bytes, 0, intPtr, bytes.Length);
            return new FLSlice(intPtr.ToPointer(), (ulong)bytes.Length);
        }

        public static void Free(FLSlice slice)
        {
            Marshal.FreeHGlobal(new IntPtr(slice.buf));
            slice.buf = null;
            slice.size = 0;
        }

        public string CreateString()
        {
            if(buf == null) {
                return null;
            }


            var tmp = new IntPtr(buf);
            var bytes = new byte[size];
            Marshal.Copy(tmp, bytes, 0, bytes.Length);
            return Encoding.UTF8.GetString(bytes, 0, bytes.Length);
        }

        public override int GetHashCode()
        {
            var hasher = Hasher.Start.Add(size);
            var ptr = (byte*)buf;
            if (ptr != null) {
                hasher.Add(ptr[size - 1]);
            }

            return hasher.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if(!(obj is FLSlice)) {
                return false;
            }

            return Native.FLSlice_Compare(this, (FLSlice)obj) == 0;
        }

        public static explicit operator FLSliceResult(FLSlice input)
        {
            return new FLSliceResult(input.buf, input.size);
        }

        public static explicit operator C4Slice(FLSlice input)
        {
            return new C4Slice(input.buf, input.size);
        }

        public static explicit operator C4SliceResult(FLSlice input)
        {
            return new C4SliceResult(input.buf, input.size);
        }
    }

#if LITECORE_PACKAGED
    internal
#else
    public
#endif
         unsafe partial struct FLSliceResult : IDisposable
    {
        public FLSliceResult(void* buf, ulong size)
        {
            this.buf = buf;
            _size = (UIntPtr)size;
        }

        public static explicit operator FLSlice(FLSliceResult input)
        {
            return new FLSlice(input.buf, input.size);
        }

        public static explicit operator C4Slice(FLSliceResult input)
        {
            return new C4Slice(input.buf, input.size);
        }

        public static explicit operator C4SliceResult(FLSliceResult input)
        {
            return new C4SliceResult(input.buf, input.size);
        }

        public void Dispose()
        {
            Native.FLSliceResult_Free(this);
        }
    }

#if LITECORE_PACKAGED
    internal
#else
    public
#endif
        static unsafe class FLSliceExtensions
    {
        public static object ToObject(FLValue* value)
        {
            if (value == null) {
                return null;
            }

            switch (Native.FLValue_GetType(value)) {
                case FLValueType.Array:
                {
                    var arr = Native.FLValue_AsArray(value);
                    var retVal = new object[Native.FLArray_Count(arr)];
                    if (retVal.Length == 0) {
                        return retVal;
                    }
                    
                    FLArrayIterator i;
                    Native.FLArrayIterator_Begin(arr, &i);
                    int pos = 0;
                    do {
                        retVal[pos++] = ToObject(Native.FLArrayIterator_GetValue(&i));
                    } while (Native.FLArrayIterator_Next(&i));

                    return retVal;
                }
                case FLValueType.Boolean:
                    return Native.FLValue_AsBool(value);
                case FLValueType.Data:
                    return Native.FLValue_AsData(value);
                case FLValueType.Dict:
                {
                    var dict = Native.FLValue_AsDict(value);
                    var count = (int) Native.FLDict_Count(dict);
                    var retVal = new Dictionary<string, object>(count);
                    if (count == 0) {
                        return retVal;
                    }

                    FLDictIterator i;
                    Native.FLDictIterator_Begin(dict, &i);
                    do {
                        var rawKey = Native.FLDictIterator_GetKey(&i);
                        string key = Native.FLValue_AsString(rawKey);
                        if (key == null) {
                            break;
                        }

                        retVal[key] = ToObject(Native.FLDictIterator_GetValue(&i));
                    } while (Native.FLDictIterator_Next(&i));

                    return retVal;
                }
                case FLValueType.Null:
                    return null;
                case FLValueType.Number:
                    if (Native.FLValue_IsInteger(value)) {
                        if (Native.FLValue_IsUnsigned(value)) {
                            return Native.FLValue_AsUnsigned(value);
                        }

                        return Native.FLValue_AsInt(value);
                    } else if (Native.FLValue_IsDouble(value)) {
                        return Native.FLValue_AsDouble(value);
                    }

                    return Native.FLValue_AsFloat(value);
                case FLValueType.String:
                    return Native.FLValue_AsString(value);
                default:
                    return null;
            }
        }

        public static FLSliceResult FLEncode(this object obj)
        {
            var enc = Native.FLEncoder_New();
            try {
                obj.FLEncode(enc);
                FLError err;
                var retVal = NativeRaw.FLEncoder_Finish(enc, &err);
                if (retVal.buf == null) {
                    throw new CouchbaseFleeceException(err);
                }

                return retVal;
            } finally {
                Native.FLEncoder_Free(enc);
            }
        }

        public static void FLEncode<TVal>(this IDictionary<string, TVal> dict, FLEncoder* enc)
        {
            if (dict == null) {
                Native.FLEncoder_WriteNull(enc);
                return;
            }

            Native.FLEncoder_BeginDict(enc, (ulong) dict.Count);
            foreach (var pair in dict) {
                Native.FLEncoder_WriteKey(enc, pair.Key);
                pair.Value.FLEncode(enc);
            }

            Native.FLEncoder_EndDict(enc);
        }

        public static void FLEncode(this IList list, FLEncoder* enc)
        {
            if (list == null) {
                Native.FLEncoder_WriteNull(enc);
                return;
            }

            Native.FLEncoder_BeginArray(enc, (ulong)list.Count);
            foreach (var obj in list) {
                obj.FLEncode(enc);
            }

            Native.FLEncoder_EndArray(enc);
        }

        public static void FLEncode(this string str, FLEncoder* enc)
        {
            Native.FLEncoder_WriteString(enc, str);
        }

        public static void FLEncode(this IEnumerable<byte> str, FLEncoder* enc)
        {
            Native.FLEncoder_WriteData(enc, str.ToArray());
        }

        public static void FLEncode(this double d, FLEncoder* enc)
        {
            Native.FLEncoder_WriteDouble(enc, d);
        }

        public static void FLEncode(this float f, FLEncoder* enc)
        {
            Native.FLEncoder_WriteFloat(enc, f);
        }

        public static void FLEncode(this long l, FLEncoder* enc)
        {
            Native.FLEncoder_WriteInt(enc, l);
        }

        public static void FLEncode(this ulong u, FLEncoder* enc)
        {
            Native.FLEncoder_WriteUInt(enc, u);
        }

        public static void FLEncode(this bool b, FLEncoder* enc)
        {
            Native.FLEncoder_WriteBool(enc, b);
        }

        public static void FLEncode(this object obj, FLEncoder* enc)
        {
            switch (obj) {
                case null:
                    Native.FLEncoder_WriteNull(enc);
                    break;
                case IFLEncodable flObj:
                    flObj.FLEncode(enc);
                    break;
                case IDictionary<string, object> dict:
                    dict.FLEncode(enc);
                    break;
                case IDictionary<string, string> dict:
                    dict.FLEncode(enc);
                    break;
                case IEnumerable<byte> data:
                    data.FLEncode(enc);
                    break;
                case IList list:
                    list.FLEncode(enc);
                    break;
                case string s:
                    s.FLEncode(enc);
                    break;
                case uint u:
                    ((ulong) u).FLEncode(enc);
                    break;
                case ulong u:
                    u.FLEncode(enc);
                    break;
                case int i:
                    ((long) i).FLEncode(enc);
                    break;
                case long l:
                    l.FLEncode(enc);
                    break;
                case float f:
                    f.FLEncode(enc);
                    break;
                case double d:
                    d.FLEncode(enc);
                    break;
                case bool b:
                    b.FLEncode(enc);
                    break;
                case DateTimeOffset dto:
                    (dto.ToString("o")).FLEncode(enc);
                    break;
                default:
                    if (_FLEncodeExtension?.Invoke(obj, enc) != true) {
                        throw new InvalidCastException($"Cannot encode {obj.GetType().FullName} to Fleece!");
                    }

                    break;
            }
        }

        public delegate bool FLEncodeExtension(object obj, FLEncoder* encoder);

        private static FLEncodeExtension _FLEncodeExtension;
        public static void RegisterFLEncodeExtension(FLEncodeExtension extension)
        {
            _FLEncodeExtension = extension;
        }
    }
}
