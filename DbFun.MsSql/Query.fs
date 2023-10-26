namespace DbFun.MsSql.Builders

open DbFun.Core.Builders
open DbFun.MsSql.Builders
open System.Data

[<AutoOpen>]
module Extensions = 

    type QueryConfig with
        static member MsSqlDefault(createConnection: unit -> IDbConnection): QueryConfig = 
            { QueryConfig.Default(createConnection) with
                ParamBuilders = ParamsImpl.getDefaultBuilders(createConnection >> unbox)
            }

type QueryBuilder(config: QueryConfig) =
    inherit DbFun.Core.Builders.QueryBuilder(config)

    new(createConnection: unit -> IDbConnection) = 
        QueryBuilder(QueryConfig.MsSqlDefault(createConnection))

    member this.Timeout(timeout: int) = 
        QueryBuilder({ this.Config with Timeout = Some timeout })

    member this.LogCompileTimeErrors() = 
        QueryBuilder({ this.Config with LogCompileTimeErrors = true })
