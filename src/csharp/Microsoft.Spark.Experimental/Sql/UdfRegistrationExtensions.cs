// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using System;
using Apache.Arrow;
using Microsoft.Spark.Utils;

namespace Microsoft.Spark.Sql
{
    public static class UdfRegistrationExtensions
    {
        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T">Specifies the type of the first argument to the Vector UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the Vector UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The Vector UDF name.</param>
        /// <param name="f">The Vector UDF function implementation.</param>
        public static void RegisterVector<T, TResult>(
            this UdfRegistration udf, string name, Func<T, TResult> f)
            where T : IArrowArray
            where TResult : IArrowArray
        {
            udf.Register<TResult>(
                name,
                UdfUtils.CreateUdfWrapper(f),
                UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF);
        }

        /// <summary>
        /// Registers the given delegate as a vector user-defined function with the specified name.
        /// </summary>
        /// <typeparam name="T1">Specifies the type of the first argument to the Vector UDF.</typeparam>
        /// <typeparam name="T2">Specifies the type of the second argument to the Vector UDF.</typeparam>
        /// <typeparam name="TResult">Specifies the return type of the Vector UDF.</typeparam>
        /// <param name="udf">The <see cref="UdfRegistration"/> object to invoke the register the Vector UDF.</param>
        /// <param name="name">The Vector UDF name.</param>
        /// <param name="f">The Vector UDF function implementation.</param>
        public static void RegisterVector<T1, T2, TResult>(
            this UdfRegistration udf, string name, Func<T1, T2, TResult> f)
            where T1 : IArrowArray
            where T2 : IArrowArray
            where TResult : IArrowArray
        {
            udf.Register<TResult>(
                name,
                UdfUtils.CreateUdfWrapper(f),
                UdfUtils.PythonEvalType.SQL_SCALAR_PANDAS_UDF);
        }
    }
}
