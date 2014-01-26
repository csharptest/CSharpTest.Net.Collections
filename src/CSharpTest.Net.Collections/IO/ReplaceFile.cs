#region Copyright 2010-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.Globalization;
using CSharpTest.Net.Collections;
using CSharpTest.Net.Utils;

namespace CSharpTest.Net.IO
{
    /// <summary>
    /// Creates a temp file based on the given file being replaced and when a call to Commit() is 
    /// made the target file is replaced with the current contents of the temporary file.  Use the
    /// TransactFile if you MUST be certain to succeed then Commit(), otherwise this implementation
    /// provides a 'good-enough' transaction and is optimized for larger files.
    /// </summary>
    public class ReplaceFile : TempFile, Interfaces.ITransactable
    {
        Stream _lock;
        bool _committed;
        readonly bool _created;
        readonly string _targetFile;
        readonly string _backupExt;

        /// <summary>
        /// Derives a new filename that doesn't exist from the provided name, ie. file.txt becomes file.txt.~0001
        /// </summary>
        /// <param name="originalPath">the name of the file</param>
        /// <param name="tempfilePath">[out] the temp file name</param>
        /// <returns>A stream with exclusive write access to the file</returns>
        public static Stream CreateDerivedFile(string originalPath, out string tempfilePath)
        {
            string tempFile = null;
            int tempInt, ordinal = 0;
            int maxAttempt = 10;

            if (!Path.IsPathRooted(originalPath))
                originalPath = Path.GetFullPath(originalPath);

            SetList<String> existingFiles = new SetList<string>(
                Directory.GetFiles(Path.GetDirectoryName(originalPath), Path.GetFileName(originalPath) + ".~????", SearchOption.TopDirectoryOnly)
                );
            if (existingFiles.Count > 0)
            {
                string extension = Path.GetExtension(existingFiles[existingFiles.Count - 1]);
                if (extension.Length == 6 && int.TryParse(extension.Substring(2), NumberStyles.AllowHexSpecifier, null, out tempInt))
                    ordinal = Math.Max(ordinal, tempInt);
            }
            Stream stream = null;
            while (stream == null)
            {
                while (tempFile == null || File.Exists(tempFile))
                    tempFile = String.Format("{0}.~{1:x4}", originalPath, ++ordinal);

                try
                {
                    stream = File.Open(tempFile, FileMode.CreateNew, FileAccess.Write, FileShare.Read);
                }
                catch (System.IO.IOException)
                {
                    if (maxAttempt-- <= 0)
                        throw;
                    tempFile = null;
                    stream = null;
                }
            }

            tempfilePath = tempFile;
            return stream;
        }

        /// <summary>
        /// Creates a temp file based on the given file being replaced and when a call to Commit() is 
        /// made the target file is replaced with the current contents of the temporary file.
        /// </summary>
        public ReplaceFile(string targetName) : base(null)
        {
            if (!Path.IsPathRooted(targetName))
                targetName = Path.GetFullPath(targetName);

            //hold an exclusive write-lock until we are committed.
            _created = !File.Exists(targetName);
            _lock = File.Open(targetName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read);
            try
            {
                _committed = false;

                string tempFile;
                CreateDerivedFile(targetName, out tempFile).Dispose();

                base.TempPath = tempFile;
                _targetFile = targetName;
                _backupExt = null;
            }
            catch 
            { this.Dispose(); throw; }
        }

        /// <summary>
        /// Creates a backup of the target file when replacing using the extension provided
        /// </summary>
        /// <param name="targetName">The name of the file to replace</param>
        /// <param name="backupExtension">A valid file extension beginning with '.'</param>
        public ReplaceFile(string targetName, string backupExtension)
            : this(targetName)
        {
            try
            {
                if (!FileUtils.IsValidExtension(backupExtension))
                    throw new ArgumentException(Resources.InvalidFileExtension(backupExtension));
                _backupExt = backupExtension;
            }
            catch
            { this.Dispose(); throw; }
        }

        /// <summary>
        /// Returns the originally provided filename that is being replaced
        /// </summary>
        public string TargetFile { get { return _targetFile; } }
        
        /// <summary>
        /// Commits the replace operation on the file
        /// </summary>
        public void Commit() { _committed = true; Dispose(true); }
        
        /// <summary> 
        /// Aborts the operation and reverts pending changes 
        /// </summary>
        public void Rollback()
        {
            Check.Assert<InvalidOperationException>(_committed == false);
            Dispose(true);
        }

        /// <summary>
        /// Disposes of the open stream and the temporary file.
        /// </summary>
        protected override void Dispose(bool disposing)
        {
            if (_lock != null)
            {
                //here is the optimistic part, if someone else interrupts this process and locks
                //the _targetfile we are going to fail.
                _lock.Dispose();
                _lock = null;

                if (_committed && disposing)
                {
                    string backupFile = null;
                    if (_backupExt != null)
                        backupFile = Path.ChangeExtension(_targetFile, _backupExt);

                    //doesn't work in Win95, but the what does?
                    File.Replace(TempPath, _targetFile, backupFile, true);
                }
                else if (_created && !_committed && disposing)
                {
                    try { File.Delete(_targetFile); }
                    catch { }
                }
            }
            base.Dispose(disposing);
        }
    }
}
