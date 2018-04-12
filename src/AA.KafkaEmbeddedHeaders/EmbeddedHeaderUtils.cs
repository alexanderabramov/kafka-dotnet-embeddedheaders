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
// Derived from: spring-cloud-stream, licensed under Apache 2.0
//
// Refer to LICENSE for more information.

using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;

namespace AA.KafkaEmbeddedHeaders
{
	/// <summary>
	/// Encodes requested headers into payload with format
	/// <code>0xff, n(1), [ [lenHdr(1), hdr, lenValue(4), value] ... ]</code>
	/// The 0xff is a magic byte;
	/// n is number of headers (max 255); 
	/// for each header, the name length (1 byte) is followed by the name, 
	/// followed by the value length (int, big endian) followed by the value (json).
	/// </summary>
	public static class EmbeddedHeaderUtils
	{
		private const int MAGIC_BYTE = 0xff;

		/// <summary>
		/// Test if the payload might potentially have embedded headers according to the format.
		/// </summary>
		public static bool MayHaveEmbeddedHeaders(byte[] bytes)
		{
			return bytes.Length > 8 && (bytes[0] & MAGIC_BYTE) == MAGIC_BYTE;
		}

		/// <summary>
		/// Extract embedded headers from the payload in an efficient manner, without creating extra copies or converting to String.
		/// </summary>
		public static (IList<Header> headers, ArraySegment<byte> body) ExtractHeaders(byte[] payload)
		{
			Contract.Requires(MayHaveEmbeddedHeaders(payload));

			byte nHeaders = payload[1];

			var headers = new Header[nHeaders];
			int index = 2;
			for (byte i = 0; i < nHeaders; ++i)
			{
				byte headerNameLength = payload[index];
				index++;
				var headerName = new ArraySegment<byte>(payload, index, headerNameLength);
				index += headerNameLength;
				// big-endian (network byte order)
				Int32 headerValueLength = (payload[index++] << 24) | (payload[index++] << 16) | (payload[index++] << 8) | payload[index++];
				var headerValue = new ArraySegment<byte>(payload, index, headerValueLength);
				index += headerValueLength;
				headers[i] = new Header(headerName, headerValue);
			}
			var body = new ArraySegment<byte>(payload, index, payload.Length - index);
			return (headers, body);
		}

		public static byte[] EmbedHeaders(IList<Header> headers, byte[] body)
		{
			Contract.Requires(headers != null);
			Contract.Requires(body != null);

			byte nHeaders = (byte)headers.Count;
			int headersLength = 0;
			for (int i = 0; i < nHeaders; ++i)
			{
				var header = headers[i];
				headersLength += header.Name.Length + header.Value.Length + 5;
			}

			byte[] payload = new byte[2 + headersLength + body.Length];

			payload[0] = 0xff;
			payload[1] = nHeaders;
			int index = 2;
			for (int i = 0; i < nHeaders; ++i)
			{
				var header = headers[i];
				var headerName = header.Name;
				var headerNameLength = headerName.Length;
				payload[index] = (byte)headerNameLength;
				index++;
				Array.Copy(headerName, 0, payload, index, headerNameLength);
				index += headerNameLength;
				var headerValue = header.Value;
				var headerValueLength = headerValue.Length;
				// big-endian (network byte order)
				payload[index++] = (byte)(headerValueLength >> 24 & 0xff);
				payload[index++] = (byte)(headerValueLength >> 16 & 0xff);
				payload[index++] = (byte)(headerValueLength >> 8 & 0xff);
				payload[index++] = (byte)(headerValueLength & 0xff);
				Array.Copy(headerValue, 0, payload, index, headerValueLength);
				index += headerValueLength;
			}

			Array.Copy(body, 0, payload, index, body.Length);

			return payload;
		}
	}
}
