namespace MoreSqlFun.MsSql.Builders

open System.Data.Common
open MoreSqlFun.Core.Builders
open MoreSqlFun.MsSql.Builders
open MoreSqlFun.Core.Diagnostics

type QueryBuilder(
        createConnection        : unit -> DbConnection, 
        ?executor               : Queries.ICommandExecutor,
        ?paramBuilders          : ParamsImpl.IBuilder list,
        ?outParamBuilders       : OutParamsImpl.IBuilder list,
        ?rowBuilders            : RowsImpl.IBuilder list,
        ?compileTimeErrorLog    : CompileTimeErrorLog Ref,
        ?timeout                : int) =

    inherit MoreSqlFun.Core.Builders.QueryBuilder(
        createConnection, 
        ?executor               = executor,
        ?paramBuilders          = Some (defaultArg paramBuilders (ParamsImpl.getDefaultBuilders(createConnection >> unbox))),
        ?outParamBuilders       = outParamBuilders,
        ?rowBuilders            = rowBuilders,
        ?compileTimeErrorLog    = compileTimeErrorLog,
        ?timeout                = timeout)

    member this.Timeout(timeout: int) = 
        QueryBuilder(
            createConnection,
            ?executor               = executor,
            paramBuilders           = this.ParamBuilders,
            outParamBuilders        = this.OutParamBuilders,
            rowBuilders             = this.RowBuilders,
            ?compileTimeErrorLog    = compileTimeErrorLog,
            timeout                 = timeout)

    member this.LogCompileTimeErrors() = 
        QueryBuilder(
            createConnection,
            ?executor               = executor,
            paramBuilders           = this.ParamBuilders,
            outParamBuilders        = this.OutParamBuilders,
            rowBuilders             = this.RowBuilders,
            compileTimeErrorLog     = ref<CompileTimeErrorLog> [],
            ?timeout                = timeout)
