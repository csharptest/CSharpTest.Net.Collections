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
using System.Diagnostics;
using System.IO;
using CSharpTest.Net.Synchronization;

namespace CSharpTest.Net.Collections
{
    partial class BPlusTree<TKey, TValue>
    {
#if DEBUG
        const bool IsDebugBuild = true;
#else
        const bool IsDebugBuild = false;
#endif
        
        bool _validated;
        TextWriter _debugOut;

        /// <summary>
        /// Sets a text writter that the entire btree will be dumped to after every read/write/seek operation.
        /// The format is a single line of text in abbreviated form: {k1{k1,k2,k3},k4{k4,k5}}
        /// </summary>
        [Conditional("DEBUG")]
        public void DebugSetOutput(TextWriter output)
        {
            _debugOut = output;
        }

        /// <summary>
        /// Sets a boolean condition that will validate the state of the entire btree after every operation.
        /// </summary>
        [Conditional("DEBUG")]
        public void DebugSetValidateOnCheckpoint(bool validate)
        { 
            _validated = validate;
        }

        [Conditional("DEBUG")]
        private void DebugComplete(string format, params object[] args)
        {
            //Trace.TraceInformation(format, args);
            if (_debugOut != null)
            {
                using (StringWriter tmp = new StringWriter())
                {
                    Print(tmp, DebugFormat.Compact);
                    tmp.Write("  -  " + format, args);
                    _debugOut.WriteLine(tmp.ToString());
                }
            }
            if(_validated)
                Validate();
        }

        /// <summary> Print formatting for nodes </summary>
        public enum DebugFormat 
        {
            /// <summary> Full information for all nodes </summary>
            Full,
            /// <summary> Formatted new lines and tabbify, but reduced information </summary>
            Formatted,
            /// <summary> Compact single line format </summary>
            Compact 
        }

        /// <summary>
        /// Prints the entire tree to the text writer
        /// </summary>
        [Conditional("DEBUG")]
        public void Print(TextWriter output, DebugFormat format)
        {
            using (RootLock root = LockRoot(LockType.Read, "Print", true))
                Print(root.Pin, output, 0, format);
        }

        [Conditional("DEBUG")]
        private void Print(NodePin node, TextWriter output, int depth, DebugFormat format)
        {
            bool formatted = format != DebugFormat.Compact;
            string prefix = formatted ? Environment.NewLine + new String(' ', depth << 1) : "";
            output.Write("{0}{{", formatted ? " " : "");
            if (formatted) prefix += "  ";

            for (int i = 0; i < node.Ptr.Count; i++)
            {
                if(i > 0 || node.Ptr[i].IsValue)
                    output.Write("{0}{1}", prefix, node.Ptr[i].Key);
                if (formatted && node.Ptr.IsLeaf) output.Write(" = {0}", node.Ptr[i].Payload);
                if (format == DebugFormat.Full)
                {
                    output.Write(" (IsLeaf={0})", node.Ptr.IsLeaf);
                    output.Write(" (Count={0})", node.Ptr.Count);
                }

                if (node.Ptr[i].IsNode)
                {
                    using (NodePin child = _storage.Lock(node, node.Ptr[i].ChildNode))
                    {
                        Print(child, output, depth + 1, format);
#if DEBUG
                        if (format == DebugFormat.Full)
                        {
                            try { Validate(child, node, i, 1); }
                            catch (Exception ex)
                            {
                                output.WriteLine();
                                output.WriteLine("{0} Error = {1}", prefix, ex.Message);
                            }
                        }
#endif
                    }
                }
                else if (formatted)
                    output.Write("={0}", node.Ptr[i].Payload);
                if (i + 1 < node.Ptr.Count)
                    output.Write(',');
            }
            if (formatted) prefix = prefix.Substring(0, prefix.Length - 2);
            output.Write("{0}}}", prefix);
        }

        /// <summary>
        /// Forces a top-down, depth-first, crawl of the entire tree in which every node and
        /// every link or key is checked for accuracy.  Throws on error.
        /// </summary>
        [Conditional("DEBUG")]
        public void Validate()
        {
#if DEBUG
            using (RootLock root = LockRoot(LockType.Read, "Validate", true))
                Validate(root.Pin, null, int.MinValue, int.MaxValue);
#endif
        }
#if DEBUG

        private int Validate(NodePin thisLock, NodePin parent, int parentIx, int depthToValidate)
        {
            Assert(thisLock != null, "Null node lock encountered.");
            Node me = thisLock.Ptr;
            Assert(me != null, "Null node reference encountered.");

            if (parent == null || parent.Ptr.IsRoot)
            { }
            else
            {
                if (me.IsLeaf)
                {
                    Assert(me.Count >= _options.MinimumValueNodes, "Not enough child nodes.");
                    Assert(me.Count <= _options.MaximumValueNodes, "Too many child nodes.");
                }
                else
                {
                    Assert(me.Count >= _options.MinimumChildNodes, "Not enough child nodes.");
                    Assert(me.Count <= _options.MaximumChildNodes, "Too many child nodes.");
                }

                Assert(parent.Ptr[parentIx].ChildNode.Equals(thisLock.Handle), "Parent index is incorrect.");
                //Invalid assumption, the meaning of Key as [0] is undefined except in leaf nodes.
                //Assert(parent.Ptr[parentIx].Key.CompareTo(List[0].Key) == 0, "My first key not in parent.");
            }

            int depth = 0;
            for (int i = 0; i < me.Count; i++)
            {
                if (parent != null)
                {
                    int ordinal;
                    if (me.IsLeaf || i > 0)
                    {
                        parent.Ptr.BinarySearch(_itemComparer, me[i], out ordinal);
                        Assert(ordinal == parentIx, "My child is not within parent range.");
                    }
                    else Assert(_keyComparer.Compare(me[i].Key, default(TKey)) == 0, "First key non-empty?");
                }

                Assert(me.IsLeaf == !me[i].IsNode, "Child container in leaf node.");
                Assert(me.IsLeaf == me[i].IsValue, "Leaf child is not a value.");
                if (!me.IsLeaf && depthToValidate > 0)
                {
                    int testDepth;
                    using (NodePin node = _storage.Lock(thisLock, me[i].ChildNode))
                        testDepth = Validate(node, thisLock, i, depthToValidate - 1);
                    if (i == 0)
                        depth = testDepth;
                    else
                        Assert(depth == testDepth, "Expected each node to have the same depth");
                }
            }
            for (int i = me.Count; i < me.Size && !me.IsRoot; i++)
            {
                Assert(me[i].IsEmpty, "Non-cleared element value.");
                bool isKeyClass = ReferenceEquals(default(TKey), null);
                Assert(isKeyClass 
                    ? ReferenceEquals(me[i].Key, null) 
                    : _keyComparer.Compare(me[i].Key, default(TKey)) == 0, "Non-cleared element key.");
            }
            return depth + 1;
        }
#endif
    }
}
