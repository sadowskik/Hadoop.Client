// Copyright (c) Microsoft Corporation
// All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
//
// THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
// WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
// MERCHANTABLITY OR NON-INFRINGEMENT.
//
// See the Apache Version 2.0 License for specific language governing
// permissions and limitations under the License.

using System;
using Newtonsoft.Json.Linq;

namespace Hadoop.Client.WebHdfs
{
    // todo - make abstract
    public class DirectoryEntry : Resource
    {
        // todo - should makt these immutable.
        public string AccessTime { get; set; }
        public string BlockSize { get; set; }
        public string Group { get; set; }
        public Int64 Length { get; set; }
        public string ModificationTime { get; set; }
        public string Owner { get; set; }
        public string PathSuffix { get; set; }
        // todo, replace with flag enum 
        public string Permission { get; set; }
        public int Replication { get; set; }
        // todo, replace with enum 
        public string Type { get; set; }

        public DirectoryEntry(JObject value)
        {
            AccessTime = value.Value<string>("accessTime");
            BlockSize = value.Value<string>("blockSize");
            Group = value.Value<string>("group");
            Length = value.Value<Int64>("length");
            ModificationTime = value.Value<string>("modificationTime");
            Owner = value.Value<string>("owner");
            PathSuffix = value.Value<string>("pathSuffix");
            Permission = value.Value<string>("permission");
            Replication = value.Value<int>("replication");
            Type = value.Value<string>("type");
            base.Info = value;
        }

    }
}
