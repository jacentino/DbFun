namespace DbFun.Npgsql.Builders

open DbFun.Core.Builders

[<AutoOpen>]
module Config = 
    type QueryConfig with
        /// <summary>
        /// Adds Postgres array support.
        /// </summary>
        member this.UsePostgresArrays() = 
            { this with ParamBuilders = ParamsImpl.PostgresArrayBuilder() :: this.ParamBuilders }
