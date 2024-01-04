namespace DbFun.Npgsql.Builders

open System
open DbFun.Core.Builders
open ParamsImpl

[<AutoOpen>]
module Config = 
    type QueryConfig with
        /// <summary>
        /// Adds Postgres array support.
        /// </summary>
        member this.UsePostgresArrays() = 
            let dateOnlyToDateTime (dtOnly: DateOnly) = dtOnly.ToDateTime(TimeOnly.MinValue)
            let timeOnlyToTimeSpan (tmOnly: TimeOnly) = tmOnly.ToTimeSpan()
            { this with 
                ParamBuilders =             
                    SeqItemConverter<_, _>(dateOnlyToDateTime) ::
                    SeqItemConverter<_, _>(timeOnlyToTimeSpan) ::
                    UnionSeqBuilder() ::
                    EnumSeqConverter<char>() ::
                    EnumSeqConverter<int>() ::
                    ParamsImpl.PostgresArrayBuilder() :: 
                    this.ParamBuilders }
