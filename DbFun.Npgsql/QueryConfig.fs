namespace DbFun.Npgsql.Builders

open DbFun.Core.Builders

[<AutoOpen>]
module Config = 
    type QueryConfig with
        /// <summary>
        /// Adds Postgress array support.
        /// </summary>
        member this.UsePostgressArrays() = 
            { this with ParamBuilders = ParamsImpl.PostgressArrayBuilder() :: this.ParamBuilders }
