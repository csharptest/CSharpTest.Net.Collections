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

namespace CSharpTest.Net.Reflection
{
	/// <summary>
	/// Allows setting or getting a property or field of a known type on an object via reflection
	/// </summary>
	public class PropertyValue<T> : PropertyValue
	{
		/// <summary>
		/// Provided an instance of an object and the name of a property/field this object
		/// allows you to set/get the value in that property/field
		/// </summary>
		/// <param name="instance">An instance of an object to retrieve the property/field from</param>
		/// <param name="name">The name of the property or field</param>
		public PropertyValue(object instance, string name)
			: base(instance, name)
		{
			Check.IsAssignable(typeof(T), base.Type);
		}

		/// <summary>
		/// Gets or sets the value of the property
		/// </summary>
		public new T Value
		{
			get { return (T)base.Value; }
			set { base.Value = value; }
		}
	}

	/// <summary>
	/// Allows setting or getting a property or field on an object via reflection
	/// </summary>
	public class PropertyValue : PropertyType
	{
		private readonly object _instance;

		/// <summary>
		/// Provided an instance of an object and the name of a property/field this object
		/// allows you to set/get the value in that property/field
		/// </summary>
		/// <param name="instance">An instance of an object to retrieve the property/field from</param>
		/// <param name="name">The name of the property or field</param>
		public PropertyValue(object instance, string name)
			: base(Check.NotNull(instance).GetType(), name)
		{
			_instance = Check.NotNull(instance);
		}

		/// <summary>
		/// Gets or sets the value of the property
		/// </summary>
		public object Value
		{
			get
			{
				return base.GetValue(_instance);
			}
			set
			{
				base.SetValue(_instance, Check.IsAssignable(this.Type, value));
			}
		}

		/// <summary>
		/// Walks a heirarchy of properties from the given type down.  You can specify in any of the 
		/// following ways: "ClientRectangle.X", "ClientRectangle/X"
		/// </summary>
		/// <example>
		/// <code>
		/// //dotted notation:
		/// PropertyValue pt = PropertyValue.TraverseProperties(this.TopLevelControl, "ClientRectangle.X");
		/// //path notation:
		/// PropertyValue pt = PropertyValue.TraverseProperties(this.TopLevelControl, "ClientRectangle/X");
		/// //individual names:
		/// PropertyValue pt = PropertyValue.TraverseProperties(this.TopLevelControl, "ClientRectangle", "X");
		/// </code>
		/// </example>
		/// <param name="instance">Any object to begin the traverse from</param>
		/// <param name="propertyNames">The name of the properties or fields usually '.' delimited</param>
		public static PropertyValue TraverseProperties(object instance, params string[] propertyNames)
		{
			PropertyValue prop = null;

			foreach (string propnames in Check.NotEmpty(propertyNames))
			{
				foreach (string name in Check.NotEmpty(propnames).Split('.', '/', '\\'))
				{
					prop = new PropertyValue(instance, Check.NotEmpty(name));
					instance = prop.Value;
				}
			}

			return Check.NotNull(prop);
		}
	}
}
