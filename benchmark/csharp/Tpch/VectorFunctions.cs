// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using System.Text;
using Apache.Arrow;

namespace Tpch
{
    internal static class VectorFunctions
    {
        internal static DoubleArray ComputeTotal(DoubleArray price, DoubleArray discount, DoubleArray tax)
        {
            if ((price.Length != discount.Length) || (price.Length != tax.Length))
            {
                throw new ArgumentException("Arrays need to be the same length.");
            }

            int length = price.Length;
            var builder = new ArrowBuffer.Builder<double>(length);
            ReadOnlySpan<double> prices = price.Values;
            ReadOnlySpan<double> discounts = discount.Values;
            ReadOnlySpan<double> taxes = tax.Values;
            for (int i = 0; i < length; ++i)
            {
                builder.Append(prices[i] * (1 - discounts[i]) * (1 + taxes[i]));
            }

            return new DoubleArray(
                builder.Build(),
                nullBitmapBuffer: ArrowBuffer.Empty,
                length: length,
                nullCount: 0,
                offset: 0);
        }

        internal static DoubleArray ComputeDiscountPrice(DoubleArray price, DoubleArray discount)
        {
            if (price.Length != discount.Length)
            {
                throw new ArgumentException("Arrays need to be the same length.");
            }

            int length = price.Length;
            var builder = new ArrowBuffer.Builder<double>(length);
            ReadOnlySpan<double> prices = price.Values;
            ReadOnlySpan<double> discounts = discount.Values;
            for (int i = 0; i < length; ++i)
            {
                builder.Append(prices[i] * (1 - discounts[i]));
            }

            return new DoubleArray(
                builder.Build(),
                nullBitmapBuffer: ArrowBuffer.Empty,
                length: length,
                nullCount: 0,
                offset: 0);
        }

        internal static StringArray GetYear(StringArray dates)
        {
            if (dates.NullCount > 0)
            {
                throw new ArgumentException("Dates cannot have null values.", nameof(dates));
            }

            int length = dates.Length;
            var valueOffsets = new ArrowBuffer.Builder<int>();
            var valueBuffer = new ArrowBuffer.Builder<byte>();
            var offset = 0;

            for (int i = 0; i < length; ++i)
            {
                valueOffsets.Append(offset);
                valueBuffer.Append(dates.GetBytes(i).Slice(0, 4));
                offset += 4;
            }

            return new StringArray(
                length,
                valueOffsets.Build(),
                valueBuffer.Build(),
                nullBitmapBuffer: ArrowBuffer.Empty);
        }

        private static readonly byte[] s_brazilUtf8 = Encoding.UTF8.GetBytes("BRAZIL");

        internal static DoubleArray IsBrazil(StringArray names, DoubleArray volumes)
        {
            if (names.Length != volumes.Length)
            {
                throw new ArgumentException("Arrays need to be the same length.");
            }

            if (volumes.NullCount > 0)
            {
                throw new ArgumentException("Volumes cannot have null values.", nameof(volumes));
            }

            int length = names.Length;
            var builder = new ArrowBuffer.Builder<double>(length);

            for (int i = 0; i < length; ++i)
            {
                if (names.IsValid(i) && names.GetBytes(i).SequenceEqual(s_brazilUtf8))
                {
                    builder.Append(volumes.Values[i]);
                }
                else
                {
                    builder.Append(0);
                }
            }

            return new DoubleArray(
                builder.Build(),
                nullBitmapBuffer: ArrowBuffer.Empty,
                length: length,
                nullCount: 0,
                offset: 0);
        }
    }
}
