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

using System.Collections.Generic;
using System.Diagnostics;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
        [DebuggerDisplay("{Key} = {_child}")]
        private struct Element
        {
            public readonly TKey Key;
            private readonly object _child;

            public Element(TKey k)
            {
                Key = k;
                _child = null;
            }

            public Element(TKey k, NodeHandle ch)
            {
                Key = k;
                _child = ch;
            }

            public Element(TKey k, TValue value)
            {
                Key = k;
                _child = value;
            }

            public Element(TKey k, Element value)
            {
                Key = k;
                _child = value._child;
            }

            public bool IsNode => _child is NodeHandle;

            public bool IsValue // Fix for null child value
                => !(_child is NodeHandle);

            public bool IsEmpty => ReferenceEquals(null, _child);
            public NodeHandle ChildNode => (NodeHandle) _child;
            public TValue Payload => (TValue) _child;

            public KeyValuePair<TKey, TValue> ToKeyValuePair()
            {
                return new KeyValuePair<TKey, TValue>(Key, Payload);
            }
        }

        private class ElementComparer : IComparer<Element>
        {
            private readonly IComparer<TKey> _keyCompare;

            public ElementComparer(IComparer<TKey> keyCompare)
            {
                _keyCompare = keyCompare;
            }

            public int Compare(Element x, Element y)
            {
                return _keyCompare.Compare(x.Key, y.Key);
            }
        }
    }
}