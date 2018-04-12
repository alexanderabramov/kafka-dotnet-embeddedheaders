// Copyright 2018 the original author or authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Refer to LICENSE for more information.

using System;

namespace AA.KafkaEmbeddedHeaders
{
	/// <summary>
	/// Represents a header. Allows initial lazy zero-copy creation in case the headers won't be requested.
	/// Not thread-safe.
	/// </summary>
	public class Header
	{
		private ArraySegment<byte> _lazyName;
		private ArraySegment<byte> _lazyValue;

		private byte[] _name;
		private byte[] _value;

		public ArraySegment<byte> LazyName => _lazyName;
		public ArraySegment<byte> LazyValue => _lazyValue;

		public byte[] Name
		{
			get
			{
				if (_name == null)
				{
					_name = new byte[_lazyName.Count];
					Array.Copy(_lazyName.Array, _lazyName.Offset, _name, 0, _lazyName.Count);
				}
				return _name;
			}
		}

		public byte[] Value
		{
			get
			{
				if (_value == null)
				{
					_value = new byte[_lazyValue.Count];
					Array.Copy(_lazyValue.Array, _lazyValue.Offset, _value, 0, _lazyValue.Count);
				}
				return _value;
			}
		}

		public Header(ArraySegment<byte> name, ArraySegment<byte> value)
		{
			_lazyName = name;
			_lazyValue = value;
		}

		public Header(byte[] name, byte[] value)
		{
			_name = name;
			_value = value;
		}
	}
}
