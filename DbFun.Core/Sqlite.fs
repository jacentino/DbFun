namespace DbFun.Core

open System
open DbFun.Core.Builders

module Sqlite = 

    type QueryConfig with
        /// <summary>
        /// Adds converters between DateTime and String.
        /// </summary>
        member this.SqliteDateTimeAsString() = 
            this.AddParamConverter(fun (v: DateTime) -> v.ToString("yyyy-MM-dd HH:mm:ss.ffffff"))
                .AddRowConverter(fun str -> DateTime.ParseExact(str, "yyyy-MM-dd HH:mm:ss.ffffff", null))
        /// <summary>
        /// Adds converters between DateTime and Int.
        /// </summary>
        member this.SqliteDateTimeAsInt() = 
            this.AddParamConverter(fun (v: DateTime) -> v.Subtract(DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).TotalSeconds |> int64)
                .AddRowConverter(fun (ts: int64) -> DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(float ts))
            