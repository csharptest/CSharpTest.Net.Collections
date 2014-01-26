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

namespace CSharpTest.Net.Interfaces
{
    /// <summary>
    /// A static singleton and factory that uses a globally common instance.
    /// </summary>
    public static class Singleton<T> where T : new()
    {
        /// <summary>
        /// Returns the singleton instance of T
        /// </summary>
        public static T Instance
        {
            get
            {
                if (Lazy._error != null)
                    throw new ApplicationException(Resources.FailedToConstructSingleton(typeof(T)), Lazy._error);
                return Lazy._instance;
            }
        }

        private class Lazy
        {
            static Lazy()
            {
                try
                {
                    _error = null;
                    _instance = new T();
                }
                catch (Exception error)
                {
                    _error = error;
                }
            }

            public static readonly T _instance;
            public static readonly Exception _error;
        }

        /// <summary>
        /// Returns a factory that returns the singleton instance
        /// </summary>
        public static IFactory<T> Factory { get { return FactoryImpl._instance; } }

        private class FactoryImpl : IFactory<T>
        {
            T IFactory<T>.Create() { return Instance; }
            static FactoryImpl() { _instance = new FactoryImpl(); }
            public static readonly FactoryImpl _instance;
        }
    }

    /// <summary>
    /// A factory that creates a new instance of an object each time Create() is called.
    /// </summary>
    public class NewFactory<T> : IFactory<T>
        where T : new()
    {
        /// <summary> Returns a new instance of T </summary>
        public T Create()
        {
            return new T();
        }
    }

    /// <summary>
    /// A delegate that takes no arguemnts and returns a single value
    /// </summary>
    public delegate T FactoryMethod<T>();

    /// <summary>
    /// A factory that creates a new instance of an object each time Create() is called.
    /// </summary>
    public class DelegateFactory<T> : IFactory<T>
    {
        private FactoryMethod<T> _method;

        /// <summary> A factory that delegates instance creation </summary>
        public DelegateFactory(FactoryMethod<T> method)
        {
            _method = method;
        }

        /// <summary> Returns an instance of T </summary>
        public T Create()
        {
            return _method();
        }
    }

    /// <summary>
    /// A factory that always returns the same instance of an object each time Create() is called.
    /// </summary>
    public class InstanceFactory<T> : IFactory<T>
    {
        private readonly T _instance;

        /// <summary> Provide the instance of T </summary>
        public InstanceFactory(T instance)
        {
            _instance = instance;
        }

        /// <summary> Returns the instance of T given to the constructor </summary>
        public T Create()
        {
            return _instance;
        }
    }
}
