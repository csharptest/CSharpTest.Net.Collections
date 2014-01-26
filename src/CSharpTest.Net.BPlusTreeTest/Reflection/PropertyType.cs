#region Copyright 2009-2014 by Roger Knapp, Licensed under the Apache License, Version 2.0
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
using System.Reflection;

namespace CSharpTest.Net.Reflection
{
	/// <summary>
	/// Allows reflection upon a property or field by name.
	/// </summary>
    public class PropertyType : ICustomAttributeProvider
	{
		private readonly Type _type;
		private readonly MemberInfo _member;

		/// <summary>
		/// Constructs the PropertyType info from a source type and an instance property or field name
		/// </summary>
		/// <param name="type">Any System.Type object to find the property or field on</param>
		/// <param name="name">The name of the property or field to find</param>
		public PropertyType(Type type, string name)
		{
			_type = Check.NotNull(type);
			Check.NotEmpty(name);

			BindingFlags baseFlags = BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy;
			//see if we can find a property by this name
			System.Type temp = _type;
			while (temp != typeof(Object) && _member == null)
			{
				_member = temp.GetProperty(name, baseFlags | BindingFlags.GetProperty | BindingFlags.SetProperty);
				if (_member == null)
					_member = temp.GetField(name, baseFlags | BindingFlags.GetField | BindingFlags.SetField);
                temp = temp.BaseType;
			}
			if (_member == null)
				throw new System.MissingMemberException(_type.FullName, name);
		}

		/// <summary>
		/// Walks a heirarchy of properties from the given type down.  You can specify in any of the 
		/// following ways: "ClientRectangle.X", "ClientRectangle/X"
		/// </summary>
		/// <example>
		/// <code>
		/// //dotted notation:
		/// PropertyType pt = PropertyType.TraverseProperties(typeof(Form), "ClientRectangle.X");
		/// //path notation:
		/// PropertyType pt = PropertyType.TraverseProperties(typeof(Form), "ClientRectangle/X");
		/// //individual names:
		/// PropertyType pt = PropertyType.TraverseProperties(typeof(Form), "ClientRectangle", "X");
		/// </code>
		/// </example>
		/// <param name="fromType">Any System.Type object to traverse from</param>
		/// <param name="propertyNames">The name of the properties or fields usually '.' delimited</param>
		public static PropertyType TraverseProperties(Type fromType, params string[] propertyNames)
		{
			PropertyType prop = null;
			Type t = fromType;

			foreach (string propnames in Check.NotEmpty(propertyNames))
			{
				foreach (string name in Check.NotEmpty(propnames).Split('.', '/', '\\'))
				{
					prop = new PropertyType(t, Check.NotEmpty(name));
					t = prop.Type;
				}
			}

			return Check.NotNull(prop);
		}

		/// <summary>
		/// Returns the name of the property/field
		/// </summary>
		public string Name { get { return _member.Name; } }

		/// <summary>
		/// Returns the type of the property/field
		/// </summary>
		public Type Type
		{
			get
			{
				if (_member is PropertyInfo)
					return ((PropertyInfo)_member).PropertyType;
				else //if (_member is FieldInfo)
					return ((FieldInfo)_member).FieldType;
			}
		}

		/// <summary>
		/// Returns the value of the property for the specified instance
		/// </summary>
		public object GetValue(object instance)
		{
			if (_member is PropertyInfo)
				return ((PropertyInfo)_member).GetValue(instance, null);
			else //if (_member is FieldInfo)
				return ((FieldInfo)_member).GetValue(instance);
		}

		/// <summary>
		/// Sets the specified value for the instance supplied
		/// </summary>
		public void SetValue(object instance, object value)
		{
			if (_member is PropertyInfo)
				((PropertyInfo)_member).SetValue(instance, value, null);
			else //if (_member is FieldInfo)
				((FieldInfo)_member).SetValue(instance, value);
		}

	    /// <summary>
	    /// Returns an array of all of the custom attributes defined on this member, excluding named 
	    /// attributes, or an empty array if there are no custom attributes.
	    /// </summary>
	    public object[] GetCustomAttributes(bool inherit)
        {
	        return _member.GetCustomAttributes(inherit);
        }

	    /// <summary>
	    /// Returns an array of custom attributes defined on this member, identified by type, or an
	    /// empty array if there are no custom attributes of that type.
	    /// </summary>
	    public object[] GetCustomAttributes(Type attributeType, bool inherit)
        {
            return _member.GetCustomAttributes(attributeType, inherit);
        }

	    /// <summary>
	    /// Indicates whether one or more instance of <paramref name="attributeType"/> is defined 
	    /// on this member.
	    /// </summary>
	    public bool IsDefined(Type attributeType, bool inherit)
        {
            return _member.IsDefined(attributeType, inherit);
        }
    }
}
