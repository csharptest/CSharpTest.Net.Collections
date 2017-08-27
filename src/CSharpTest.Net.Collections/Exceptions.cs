#region Copyright 2011 by Roger Knapp, Licensed under the Apache License, Version 2.0

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

namespace CSharpTest.Net.Collections
{
    /// <summary> The base class for BPlutTree runtime assertions </summary>

    [DebuggerStepThrough]
    [DebuggerNonUserCode]
    [CompilerGenerated]
    [GeneratedCode("CSharpTest.Net.Generators", "1.11.225.410")]
    public abstract class BaseAssertionException : Exception
    {
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
            //TODO:
            //#if DEBUG
            //            return string.Format("{0}\r\n    at {0}", new StackFrame(2, true));
            //#else
            return message;
            //#endif
        }
    }
}