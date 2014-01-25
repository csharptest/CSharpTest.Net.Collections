#region Copyright 2010-2013 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.Diagnostics;
using System.Text;

namespace CSharpTest.Net.IO
{
    /// <summary>
    /// Provides a class for managing a temporary file and making reasonable a attempt to remove it upon disposal.
    /// </summary>
    [System.Diagnostics.DebuggerDisplay("{TempPath}")]
    public class TempFile : IDisposable
    {
        private string _filename;

        /// <summary>
        /// Attaches a new instances of a TempFile to the provided file path
        /// </summary>
        public static TempFile Attach(string existingPath)
        {
            return new TempFile(existingPath);
		}
		/// <summary>
		/// Creates a temp file having the provided extension
		/// </summary>
		[DebuggerNonUserCode]
		public static TempFile FromExtension(string extensionWithDot)
		{
			return new TempFile(CreateTempPath(extensionWithDot));
		}
    	/// <summary>
        /// Creates a temp file having the provided extension
        /// </summary>
        [DebuggerNonUserCode]
		public static string CreateTempPath(string extensionWithDot)
        {
            int attempt = 0;
            while (true)
            {
                try
                {
                    string path = Path.GetTempFileName();
					if (string.IsNullOrEmpty(extensionWithDot))
					{
						if (!File.Exists(path))
							File.Open(path, FileMode.CreateNew).Dispose();
						return path;
					}
                	if (File.Exists(path))
                        File.Delete(path);
                    path = Path.ChangeExtension(path, extensionWithDot);
                    File.Open(path, FileMode.CreateNew).Dispose();
                    return path;
				}
				catch (UnauthorizedAccessException)
				{
					if (++attempt < 10)
						continue;
					throw;
				}
                catch (IOException)
                {
                    if (++attempt < 10)
                        continue;
                    throw;
                }
            }
        }
        /// <summary>
        /// Creates a temp file having a copy of the specified file
        /// </summary>
        public static TempFile FromCopy(string srcFileName)
        {
            TempFile temp = FromExtension(Path.GetExtension(srcFileName));
            try
            {
                File.Copy(srcFileName, temp.TempPath, true);
                return temp;
            }
            catch
            {
                temp.Dispose();
                throw;
            }
        }
        /// <summary>
        /// Safely delete the provided file name
        /// </summary>
        public static void Delete(string path)
        {
            bool exists = false;
            try { exists = !String.IsNullOrEmpty(path) && File.Exists(path); }
            catch (System.IO.IOException) { }

            if(exists)
                new TempFile(path).Dispose();
        }
        /// <summary>
        /// Constructs a new temp file with a newly created/empty file.
        /// </summary>
        public TempFile() : this(Path.GetTempFileName()) { }
        /// <summary>
        /// Manage the provided file path
        /// </summary>
        public TempFile(string filename)
        {
            TempPath = filename;
        }
        /// <summary>
        /// Removes the file if Dispose() is not called
        /// </summary>
        ~TempFile() { try { Dispose(false); } catch { } }
        /// <summary>
        /// Returns the temporary file path being managed.
        /// </summary>
        public string TempPath
        {
            [DebuggerNonUserCode]
            get 
            { 
                if (String.IsNullOrEmpty(_filename)) 
                    throw new ObjectDisposedException(GetType().ToString()); 
                return _filename; 
            }
            protected set
            {
                if (Exists)
                    TempFile.Delete(_filename);

                if (!String.IsNullOrEmpty(_filename = value))
                    _filename = Path.GetFullPath(_filename);
            }
        }
        /// <summary> Disposes of the temporary file </summary>
        public void Dispose() { Dispose(true); }
        /// <summary>
        /// Disposes of the temporary file
        /// </summary>
        [DebuggerNonUserCode]
        protected virtual void Dispose(bool disposing)
        {
            try
            {
                if (_filename != null && Exists)
                    File.Delete(_filename);
                _filename = null;

                if (disposing)
                    GC.SuppressFinalize(this);
            }
            catch (System.IO.IOException e)
            {
                string filename = _filename;

                if (!disposing) //wait for next GC's collection
                {
                    new TempFile(filename);
                    _filename = null;
                }

                Trace.TraceWarning("Unable to delete temp file: {0}, reason: {1}", filename, e.Message);
            }
        }

        /// <summary>
        /// Detatches this instance from the temporary file and returns the temp file's path
        /// </summary>
        public string Detatch()
        {
            GC.SuppressFinalize(this);
            string name = _filename;
            _filename = null;
            return name;
        }

        /// <summary>
        /// Returns true if the current temp file exists.
        /// </summary>
        [DebuggerNonUserCode]
        public bool Exists { get { return !String.IsNullOrEmpty(_filename) && File.Exists(_filename); }}

        /// <summary>
        /// Gets or sets the current length of the temp file.  If setting the length on a file that
        /// does not exist one will be created.  If getting the length of a file that doesnt exist
        /// zero will be returned.
        /// </summary>
        public long Length
        {
            get { return !Exists ? 0L : Info.Length; }
            set { using (Stream s = Open()) s.SetLength(value); }
        }

        /// <summary>
        /// Returns the FileInfo object for this temp file.
        /// </summary>
        public FileInfo Info { get { return new FileInfo(TempPath); } }

        /// <summary> Reads all bytes from the file </summary>
        public byte[] ReadAllBytes() { return File.ReadAllBytes(TempPath); }
        /// <summary> Writes all bytes to the file </summary>
        public void WriteAllBytes(byte[] data) { File.WriteAllBytes(TempPath, data); }
        /// <summary> Reads all UTF8 text from the file </summary>
        public string ReadAllText() { return File.ReadAllText(TempPath, Encoding.UTF8); }
        /// <summary> Writes all UTF8 text to the file </summary>
        public void WriteAllText(string content) { File.WriteAllText(TempPath, content, Encoding.UTF8); }

        /// <summary>
        /// Deletes the current temp file immediatly if it exists.
        /// </summary>
        public void Delete() { if (Exists) File.Delete(TempPath); }
        /// <summary>
        /// Re-Creates and Opens the temporary file for writing, multiple calls will truncate existing data.
        /// </summary>
        public Stream Create() { return File.Open(TempPath, FileMode.Create, FileAccess.Write, FileShare.Read); }
        /// <summary>
        /// Open or Create the temporary file for reading and writing
        /// </summary>
        public Stream Open() { return File.Open(TempPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read); }
        /// <summary>
        /// Opens the temporary file for reading
        /// </summary>
        public Stream Read() { return Read(FileShare.ReadWrite); }
        /// <summary>
        /// Opens the temporary file for reading
        /// </summary>
        public Stream Read(FileShare shared) { return File.Open(TempPath, FileMode.Open, FileAccess.Read, shared); }
        /// <summary>
        /// Copies the file content to the specified target file name
        /// </summary>
        public void CopyTo(string target) { CopyTo(target, false); }
        /// <summary>
        /// Copies the file content to the specified target file name
        /// </summary>
        public void CopyTo(string target, bool replace)
        {
            File.Copy(TempPath, target, replace);
        }
    }
}
