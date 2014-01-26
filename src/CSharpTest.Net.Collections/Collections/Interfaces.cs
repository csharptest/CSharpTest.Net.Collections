#region Copyright 2011-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#endregion
using System;
using CSharpTest.Net.Serialization;

namespace CSharpTest.Net.Collections
{
    /// <summary> Identifies a class as a reference to a node instance </summary>
    public interface IStorageHandle : IEquatable<IStorageHandle>
    { }

    /// <summary> Represents a persistance mechanic for node data </summary>
    public interface INodeStorage : ISerializer<IStorageHandle>, IDisposable
    {
        /// <summary> Returns an immutable handle to the root node, sets isNew to true if no data exists </summary>
        IStorageHandle OpenRoot(out bool isNew);

        /// <summary> Destroys the entire contents of the storage system except for the root handle which remains valid </summary>
        void Reset();

        /// <summary> Retrieves a single node from storage </summary>
        bool TryGetNode<TNode>(IStorageHandle handle, out TNode node, ISerializer<TNode> serializer);

        /// <summary> Creates a node handle that will represent a new node instance </summary>
        IStorageHandle Create();

        /// <summary> Destroys the node that was formally stored by the specified handle </summary>
        void Destroy(IStorageHandle handle);

        /// <summary> Updates the node of the specified handle with the instance given </summary>
        void Update<TNode>(IStorageHandle handle, ISerializer<TNode> serializer, TNode node);
    }

    /// <summary> An optional interface that allows storage provides to persist the record count </summary>
    public interface INodeStoreWithCount : INodeStorage, Interfaces.ITransactable
    {
        /// <summary>
        /// Used to retrieve the current record count after opening a store, -1 indicates an invalid entry.
        /// Prior to Commit() the count will be set to the actual record count.
        /// </summary>
        int Count { get; set; }
    }
}
