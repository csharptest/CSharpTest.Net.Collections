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
using System.IO;
using CSharpTest.Net.Interfaces;

namespace CSharpTest.Net.IO
{
    /// <summary>
    /// Provides a default implementation of an IFactory for creating streams on a single file.
    /// </summary>
    public class FileStreamFactory : IFactory<Stream>
    {
        const int DefaultBuffer = 4096;
        readonly string _filename;
        readonly FileMode _mode;
        readonly FileAccess _access;
        readonly FileShare _share;
        readonly int _bufferSize;
        readonly FileOptions _options;

        /// <summary> Creates an IFactory for creating streams on a single file </summary>
        public FileStreamFactory(string filename, FileMode mode)
            : this(filename, mode, mode == FileMode.Append ? FileAccess.Write : FileAccess.ReadWrite, FileShare.None, DefaultBuffer, FileOptions.None)
        { }
        /// <summary> Creates an IFactory for creating streams on a single file </summary>
        public FileStreamFactory(string filename, FileMode mode, FileAccess access)
            : this(filename, mode, access, access == FileAccess.Read ? FileShare.Read : FileShare.None, DefaultBuffer, FileOptions.None)
        { }
        /// <summary> Creates an IFactory for creating streams on a single file </summary>
        public FileStreamFactory(string filename, FileMode mode, FileAccess access, FileShare share)
            : this(filename, mode, access, share, DefaultBuffer, FileOptions.None)
        { }
        /// <summary> Creates an IFactory for creating streams on a single file </summary>
        public FileStreamFactory(string filename, FileMode mode, FileAccess access, FileShare share, int bufferSize)
            : this(filename, mode, access, share, bufferSize, FileOptions.None)
        { }
        /// <summary> Creates an IFactory for creating streams on a single file </summary>
        public FileStreamFactory(string filename, FileMode mode, FileAccess access, FileShare share, int bufferSize, FileOptions options)
        {
            _filename = Check.NotEmpty(filename);
            _mode = mode;
            _access = access;
            _share = share;
            _bufferSize = bufferSize;
            _options = options;
        }

        /// <summary> The FileName that this factory produces streams for </summary>
        public string FileName { get { return _filename; } }

        /// <summary>
        /// Creates the file stream
        /// </summary>
        public Stream Create()
        {
            return new FileStream(_filename, _mode, _access, _share, _bufferSize, _options); 
        }
    }
}
