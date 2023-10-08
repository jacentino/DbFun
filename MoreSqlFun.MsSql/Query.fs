namespace MoreSqlFun.MsSql.Builders

open System.Data.Common
open MoreSqlFun.Core.Builders
open MoreSqlFun.MsSql.Builders

type QueryBuilder(
        createConnection    : unit -> DbConnection, 
        ?executor           : Queries.ICommandExecutor,
        ?paramBuilders      : ParamsImpl.IBuilder list,
        ?outParamBuilders   : OutParamsImpl.IBuilder list,
        ?rowBuilders        : RowsImpl.IBuilder list,
        ?timeout            : int) =
    inherit MoreSqlFun.Core.Builders.QueryBuilder(
        createConnection, 
        ?executor           = executor,
        ?paramBuilders      = Some (defaultArg paramBuilders (ParamsImpl.getDefaultBuilders(createConnection >> unbox))),
        ?outParamBuilders   = outParamBuilders,
        ?rowBuilders        = rowBuilders,
        ?timeout            = timeout)

    member this.Timeout(timeout: int) = 
        QueryBuilder(
            createConnection,
            ?executor            = executor,
            paramBuilders       = defaultArg paramBuilders this.ParamBuilders,
            outParamBuilders    = defaultArg outParamBuilders this.OutParamBuilders,
            rowBuilders         = defaultArg rowBuilders this.RowBuilders,
            timeout            = timeout)
