#region Copyright 2009-2013 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.Collections.Generic;
using System.IO;
using System.Diagnostics;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text;
using System.IO.Compression;

namespace CSharpTest.Net.Utils
{
	/// <summary>
	/// Provides utilities related to files and file paths
	/// </summary>
	public static class FileUtils
	{
		private static readonly string FileNotFoundMessage;
		private static readonly char[] IllegalFileNameChars;

        static FileUtils()
        {
            FileNotFoundMessage = new FileNotFoundException().Message;

            Dictionary<Char, Char> bad = new Dictionary<Char, Char>();
            foreach (char ch in new char[] { '/', '\\', ':', '*', '?', '"', '<', '>', '|' })
                bad[ch] = ch;
            foreach (char ch in Path.GetInvalidFileNameChars())
                bad[ch] = ch;

            List<Char> badChars = new List<Char>(bad.Keys);
            badChars.Sort();
            IllegalFileNameChars = badChars.ToArray();
        }

        /// <summary>
        /// Returns true if the extension provided contains only one '.' at the beginning
        /// of the string and does not contain any path or invalid filename characters.
        /// </summary>
        public static bool IsValidExtension(string fileExt)
        {
            if (String.IsNullOrEmpty(fileExt) || fileExt.Trim() != fileExt ||
                fileExt.LastIndexOf('.') != 0 ||
                fileExt.IndexOfAny(IllegalFileNameChars) >= 0)
                return false;

            return true;
        }
        /// <summary>
        /// Returns true if the name provided contains only valid filename characters
        /// </summary>
        public static bool IsValidFileName(string filename)
        {
            if(String.IsNullOrEmpty(filename) || filename.Trim() != filename ||
                filename.Trim('.', ' ', '\t').Length == 0 ||
                filename.IndexOfAny(IllegalFileNameChars) >= 0)
                return false;

            return true;
        }

        /// <summary>
        /// Creates a valid filename by removing all invalid characters.
        /// </summary>
        public static string MakeValidFileName(string filename)
        { return MakeValidFileName(filename, String.Empty); }

        /// <summary>
        /// Creates a valid filename by replacing all invalid characters with the string provided.
        /// </summary>
        public static string MakeValidFileName(string filename, string replaceWithChars)
        {
            if (Check.NotNull(replaceWithChars).Length > 0 && !IsValidFileName(replaceWithChars))
                throw new ArgumentException();

            if (IsValidFileName(filename))
                return filename;

            StringBuilder sbpath = new StringBuilder();
            bool replaced = false;
            foreach (Char ch in filename)
            {
                bool invalid = Array.BinarySearch(IllegalFileNameChars, ch) >= 0;

                if (!replaced && invalid)
                    sbpath.Append(replaceWithChars);
                else if (!invalid)
                    sbpath.Append(ch);

                replaced = invalid;
            }

            filename = sbpath.ToString().Trim();

            if (!IsValidFileName(filename))
                throw new ArgumentException();

            return filename;
        }

		/// <summary>
		/// Returns the fully qualified path to the file if it is fully-qualified, exists in the current directory, or 
		/// in the environment path, otherwise generates a FileNotFoundException exception.
		/// </summary>
        [System.Diagnostics.DebuggerNonUserCode]
		public static string FindFullPath(string location)
		{
			string result;
			if (TrySearchPath(location, out result))
				return result;
			throw new FileNotFoundException(FileNotFoundMessage, location);
		}

		/// <summary>
		/// Expands environment variables into text, i.e. %SystemRoot%, or %ProgramFiles%
		/// </summary>
		public static String ExpandEnvironment(string input)
		{
			return Environment.ExpandEnvironmentVariables(input);
		}

		/// <summary>
		/// Returns true if the file is fully-qualified, exists in the current directory, or in the environment path, 
		/// otherwise generates a FileNotFoundException exception.  Will not propagate errors.
		/// </summary>
		public static bool TrySearchPath(string location, out string fullPath)
		{
			fullPath = null;

			try
			{
				if (File.Exists(location))
				{
					fullPath = Path.GetFullPath(location);
					return true;
				}

				if (!IsValidFileName(location))
					return false;

				foreach (string pathentry in Environment.GetEnvironmentVariable("PATH").Split(';'))
				{
					string testPath = pathentry.Trim();
					if (testPath.Length > 0 && Directory.Exists(testPath) && File.Exists(Path.Combine(testPath, location)))
					{
						fullPath = Path.GetFullPath(Path.Combine(testPath, location));
						return true;
					}
				}
			}
			catch (Exception error) { Trace.TraceError("{0}", error); }

			return false;
		}

        /// <summary>
        /// For this to work for a directory the argument should end with a '\' character
        /// </summary>
        public static string MakeRelativePath(string startFile, string targetFile)
        {
            StringBuilder newpath = new StringBuilder();

            if (startFile == null || targetFile == null)
                return null;
            if (startFile == targetFile)
                return Path.GetFileName(targetFile);
            
            List<string> sfpath = new List<string>(startFile.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
            List<string> tfpath = new List<string>(targetFile.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));

			for (int i = sfpath.Count - 1; i >= 0; i--)
				if (sfpath[i] == ".")
					sfpath.RemoveAt(i);

			for (int i = tfpath.Count - 1; i >= 0; i--)
				if (tfpath[i] == ".")
					tfpath.RemoveAt(i);

			int cmpdepth = Math.Min(sfpath.Count - 1, tfpath.Count - 1);
            int ixdiff = 0;
            for (; ixdiff < cmpdepth; ixdiff++)
                if (false == StringComparer.OrdinalIgnoreCase.Equals(sfpath[ixdiff], tfpath[ixdiff]))
                    break;

			if (ixdiff == 0 && Path.IsPathRooted(targetFile))
				return targetFile;//new volumes can't be relative

			for (int i = ixdiff; i < (sfpath.Count - 1); i++)
                newpath.AppendFormat("..{0}", Path.DirectorySeparatorChar);
			for (int i = ixdiff; i < tfpath.Count; i++)
            {
                newpath.Append(tfpath[i]);
				if ((i + 1) < tfpath.Count)
                    newpath.Append(Path.DirectorySeparatorChar);
            }
            return newpath.ToString();
        }

		/// <summary> Grants the user FullControl for the file, returns true if modified, false if already present </summary>
		public static bool GrantFullControlForFile(string filepath, WellKnownSidType sidType)
		{ return GrantFullControlForFile(filepath, sidType, null); }

		/// <summary> Grants the user FullControl for the file, returns true if modified, false if already present </summary>
		public static bool GrantFullControlForFile(string filepath, WellKnownSidType sidType, SecurityIdentifier domain)
		{
			FileSecurity sec = File.GetAccessControl(filepath);
			SecurityIdentifier sid = new SecurityIdentifier(sidType, domain);
			bool found = false;

			List<FileSystemAccessRule> toremove = new List<FileSystemAccessRule>();
			foreach (FileSystemAccessRule rule in sec.GetAccessRules(true, false, typeof(SecurityIdentifier)))
			{
				if (sid.Value == rule.IdentityReference.Value)
				{
					if (rule.AccessControlType != AccessControlType.Allow || rule.FileSystemRights != FileSystemRights.FullControl)
						toremove.Add(rule);
					else
						found = true;
				}
			}
			if (!found || toremove.Count > 0)
			{
				foreach (FileSystemAccessRule bad in toremove)
					sec.RemoveAccessRule(bad);

				sec.AddAccessRule(new FileSystemAccessRule(sid, FileSystemRights.FullControl, AccessControlType.Allow));
				File.SetAccessControl(filepath, sec);
				return true;
			}

			return false;
		}

		/// <summary> Returns the rights assigned to the given SID for this file's ACL </summary>
		public static FileSystemRights GetPermissions(string filepath, WellKnownSidType sidType)
		{
			SecurityIdentifier domain = null;
			FileSecurity sec = File.GetAccessControl(filepath);
			SecurityIdentifier sid = new SecurityIdentifier(sidType, domain);

			FileSystemRights rights = 0;
			foreach (FileSystemAccessRule rule in sec.GetAccessRules(true, false, typeof(SecurityIdentifier)))
			{
				if (sid.Value == rule.IdentityReference.Value)
				{
					if (rule.AccessControlType == AccessControlType.Allow)
						rights |= rule.FileSystemRights;
					else
						rights &= ~rule.FileSystemRights;
				}
			}

			return rights;
		}

		/// <summary> Removes any existing access for the user SID supplied and adds the specified rights </summary>
		public static void ReplacePermissions(string filepath, WellKnownSidType sidType, FileSystemRights allow)
		{
			FileSecurity sec = File.GetAccessControl(filepath);
			SecurityIdentifier sid = new SecurityIdentifier(sidType, null);
			sec.PurgeAccessRules(sid); //remove existing
			if(allow != default(FileSystemRights))
				sec.AddAccessRule(new FileSystemAccessRule(sid, allow, AccessControlType.Allow));
			File.SetAccessControl(filepath, sec);
		}
	}
}
