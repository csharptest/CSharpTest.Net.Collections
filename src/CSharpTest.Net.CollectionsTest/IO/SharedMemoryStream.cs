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
using System.IO;
using CSharpTest.Net.Interfaces;

namespace CSharpTest.Net.IO
{
    /// <summary>
    /// A memory stream that can be cloned to create an instance for another thread to access
    /// the same memory pool.  
    /// </summary>
    public class SharedMemoryStream : SegmentedMemoryStream, ICloneable, IFactory<Stream>
    {
        /// <summary>
        /// Creates a memory stream that uses 32k segments for storage
        /// </summary>
        public SharedMemoryStream()
            : base(short.MaxValue)
        { }
        /// <summary>
        /// Create a memory stream that uses the specified size of segments
        /// </summary>
        public SharedMemoryStream(int segmentSize) 
            : base(segmentSize)
        { }

        /// <summary> Creates a 'clone' of the stream sharing the same contents </summary>
        public SharedMemoryStream(SharedMemoryStream from)
            : base(Check.NotNull(from))
        { }
        /// <summary>
        /// Returns a 'clone' of this stream so that the two instances act independantly upon a single set of data
        /// </summary>
        public SharedMemoryStream Clone() { return new SharedMemoryStream(this); }
        object ICloneable.Clone() { return Clone(); }
        Stream IFactory<Stream>.Create() { return Clone(); }
    }
}