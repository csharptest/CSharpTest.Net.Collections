﻿#region Copyright 2008-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0

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
using System.Collections;
using System.Diagnostics;
using System.Reflection;

/// <summary>
///     provides a set of runtime validations for inputs
/// </summary>
[DebuggerNonUserCode]
internal static class Check
{
    /// <summary>
    ///     Used to delay creation of the excpetion until the condition fails.
    /// </summary>
    public delegate Exception ExceptionBuilder();

    /// <summary>
    ///     Verifies that the condition is true and if it fails constructs the specified type of
    ///     exception and throws.
    /// </summary>
    public static void Assert<TException>(bool condition)
        where TException : Exception, new()
    {
        if (!condition)
            throw new TException();
    }

    /// <summary>
    ///     Verifies that value is not null and returns the value or throws ArgumentNullException
    /// </summary>
    public static T NotNull<T>(T value)
    {
        if (value == null) throw new ArgumentNullException();
        return value;
    }

    /// <summary>
    ///     Verfies that the string is not null and not empty and returns the string.
    ///     throws ArgumentNullException, ArgumentOutOfRangeException
    /// </summary>
    public static string NotEmpty(string value)
    {
        if (value == null) throw new ArgumentNullException();
        if (value.Length == 0) throw new ArgumentOutOfRangeException();
        return value;
    }

    /// <summary>
    ///     Verfies that the Guid is not empty.
    ///     throws ArgumentOutOfRangeException
    /// </summary>
    public static Guid NotEmpty(Guid value)
    {
        if (value == Guid.Empty)
            throw new ArgumentOutOfRangeException();
        return value;
    }

    /// <summary>
    ///     Verfies that the collection is not null and not empty and returns the collection.
    ///     throws ArgumentNullException, ArgumentOutOfRangeException
    /// </summary>
    public static T NotEmpty<T>(T value) where T : IEnumerable
    {
        if (value == null) throw new ArgumentNullException();
        if (!value.GetEnumerator().MoveNext()) throw new ArgumentOutOfRangeException();
        return value;
    }

    /// <summary>
    ///     Verifies that the two values are the same
    ///     throws ArgumentException
    /// </summary>
    public static void IsEqual<T>(T a, T b) where T : IEquatable<T>
    {
        if (false == a.Equals(b))
            throw new ArgumentException();
    }

    /// <summary>
    ///     Verifies that the two values are NOT the same
    ///     throws ArgumentException
    /// </summary>
    public static void NotEqual<T>(T a, T b) where T : IEquatable<T>
    {
        if (a.Equals(b))
            throw new ArgumentException();
    }

    /// <summary>
    ///     Verifies that the array is not empty and has at least min, but not more than max items.
    ///     throws ArgumentNullExcpetion
    ///     throws ArgumentOutOfRangeException
    /// </summary>
    public static T[] ArraySize<T>(T[] value, int min, int max)
    {
        if (value == null) throw new ArgumentNullException();
        if (value.Length < min || value.Length > max)
            throw new ArgumentOutOfRangeException();
        return value;
    }

    /// <summary>
    ///     Verifies that the value is min, max, or between the two.
    ///     throws ArgumentOutOfRangeException
    /// </summary>
    public static T InRange<T>(T value, T min, T max) where T : IComparable<T>
    {
        if (value == null) throw new ArgumentNullException();
        if (value.CompareTo(min) < 0)
            throw new ArgumentOutOfRangeException();
        if (value.CompareTo(max) > 0)
            throw new ArgumentOutOfRangeException();
        return value;
    }

    /// <summary>
    ///     Returns (T)value if the object provided can be assinged to a variable of type T
    ///     throws ArgumentException
    /// </summary>
    public static T IsAssignable<T>(object value)
    {
        return (T) IsAssignable(typeof(T), value);
    }

    /// <summary>
    ///     Returns value if the object provided can be assinged to a variable of type toType
    ///     throws ArgumentException
    /// </summary>
    public static object IsAssignable(Type toType, object fromValue)
    {
        NotNull(toType);
        if (fromValue == null)
        {
            if (toType.GetTypeInfo().IsValueType)
                throw new ArgumentException(string.Format("Can not set value of type {0} to null.", toType));
        }
        else
        {
            IsAssignable(toType, fromValue.GetType());
        }
        return fromValue;
    }

    /// <summary>
    ///     Throws ArgumentException if the type fromType cannot be assigned to variable of type toType
    /// </summary>
    public static void IsAssignable(Type toType, Type fromType)
    {
        if (!NotNull(toType).GetTypeInfo().IsAssignableFrom(NotNull(fromType)))
            throw new ArgumentException(string.Format("Can not set value of type {0} to a value of type {1}.", toType,
                fromType));
    }
}