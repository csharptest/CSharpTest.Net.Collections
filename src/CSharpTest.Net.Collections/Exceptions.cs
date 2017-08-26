﻿#region Copyright 2011 by Roger Knapp, Licensed under the Apache License, Version 2.0

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
using System.CodeDom.Compiler;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace CSharpTest.Net
{
    /// <summary> The base class for BPlutTree runtime assertions </summary>
    [Serializable]
    [DebuggerStepThrough]
    [DebuggerNonUserCode]
    [CompilerGenerated]
    [GeneratedCode("CSharpTest.Net.Generators", "1.11.225.410")]
    public abstract class BaseAssertionException : ApplicationException
    {
        /// <summary> The base class for BPlutTree runtime assertions </summary>
        protected BaseAssertionException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }

        /// <summary> The base class for BPlutTree runtime assertions </summary>
        protected BaseAssertionException(string text)
            : base(AssertionText(text))
        {
        }

        /// <summary> The base class for BPlutTree runtime assertions </summary>
        protected BaseAssertionException(string text, Exception innerException)
            : base(AssertionText(text), innerException)
        {
        }

        [DebuggerNonUserCode]
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static string AssertionText(string message)
        {
            if (string.IsNullOrEmpty(message))
                message = Resources.ExceptionStrings.AssertionFailedException;
#if DEBUG
            return string.Format("{0}\r\n    at {0}", new StackFrame(2, true));
#else
            return message;
#endif
        }
    }

    partial class AssertionFailedException
    {
        /// <summary>
        ///     if(condition == false) throws A runtime assertion failed: {0}
        /// </summary>
        public static void Assert(bool condition, string format, params object[] args)
        {
            if (!condition)
            {
                if (args != null && args.Length > 0)
                    try
                    {
                        format = string.Format(format, args);
                    }
                    catch (Exception e)
                    {
                        format = string.Format("{0} format error: {1}", format, e.Message);
                    }
                throw new AssertionFailedException(format ?? Resources.ExceptionStrings.AssertionFailedException);
            }
        }
    }
}