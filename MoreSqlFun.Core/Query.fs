namespace MoreSqlFun.Core.Builders

open System.Data
open System.Data.Common
open MoreSqlFun.Core
open MoreSqlFun.Core.Diagnostics
open System.Runtime.CompilerServices
open System.Runtime.InteropServices
open System

module Queries = 

    type ICommandExecutor = 
        abstract member OpenConnection: DbConnection -> unit
        abstract member CreateCommand: DbConnection -> DbCommand
        abstract member Execute: DbCommand * CommandBehavior -> IDataReader
        abstract member ExecuteAsync: DbCommand * CommandBehavior  -> Async<IDataReader>

    type DefaultExecutor() = 
        interface ICommandExecutor with
            member __.OpenConnection(connection: DbConnection) = 
                connection.Open()
            member __.CreateCommand(connection: DbConnection): DbCommand = 
                connection.CreateCommand()
            member __.Execute(command: DbCommand, behavior: CommandBehavior): IDataReader = 
                command.ExecuteReader(behavior)
            member __.ExecuteAsync(command: DbCommand, behavior: CommandBehavior): Async<IDataReader> = 
                async {
                    let! token = Async.CancellationToken
                    let! reader = command.ExecuteReaderAsync(behavior, token) |> Async.AwaitTask
                    return reader
                }

type QueryConfig = 
    {
        CreateConnection    : unit -> DbConnection
        Executor            : Queries.ICommandExecutor
        ParamBuilders       : ParamsImpl.IBuilder list
        OutParamBuilders    : OutParamsImpl.IBuilder list
        RowBuilders         : RowsImpl.IBuilder list
        Timeout             : int option
        LogCompileTimeErrors: bool
    }
    with static member Default(createConnection): QueryConfig = 
            {
                CreateConnection    = createConnection
                Executor            = Queries.DefaultExecutor()
                ParamBuilders       = ParamsImpl.getDefaultBuilders()
                OutParamBuilders    = OutParamsImpl.getDefaultBuilders()
                RowBuilders         = RowsImpl.getDefaultBuilders()
                Timeout             = None
                LogCompileTimeErrors= false
            }


type QueryBuilder(config: QueryConfig) =

    let compileTimeErrorLog = if config.LogCompileTimeErrors then Some (ref<CompileTimeErrorLog> []) else None

    let executePrototypeQuery(commandType: CommandType, commandText: string, setParams: DbCommand -> unit, resultReaderBuilder: IDataReader -> IResultReader<'Result>) =
        use connection = config.CreateConnection()
        config.Executor.OpenConnection(connection)
        use command = connection.CreateCommand()
        command.CommandType <- commandType
        command.CommandText <- commandText
        setParams(command)
        use prototype = config.Executor.Execute(command, CommandBehavior.SchemaOnly)
        resultReaderBuilder(prototype)

    let executeQuery (provider: IConnector, commandText: string, resultReader: IResultReader<'Result>, setParams: DbCommand -> unit) = 
        async {
            use command = config.Executor.CreateCommand(provider.Connection)
            command.CommandType <- CommandType.Text
            command.CommandText <- commandText
            command.Transaction <- provider.Transaction
            match config.Timeout with
            | Some timeout -> command.CommandTimeout <- timeout
            | None -> ()
            setParams(command)
            use! dataReader = config.Executor.ExecuteAsync(command, CommandBehavior.Default)
            return resultReader.Read(dataReader)
        }

    let executeProcedure (provider: IConnector, commandText: string, outParamGetter: IOutParamGetter<'OutParams>, resultReader: IResultReader<'Result>, setParams: DbCommand -> unit) = 
        async {
            use command = provider.Connection.CreateCommand()
            command.CommandType <- CommandType.StoredProcedure
            command.CommandText <- commandText
            command.Transaction <- provider.Transaction
            setParams(command)
            outParamGetter.Create(command)
            use! dataReader = config.Executor.ExecuteAsync(command, CommandBehavior.Default) 
            return resultReader.Read(dataReader), outParamGetter.Get(command)
        }

    let handleException (sourcePath: string, sourceLine: int, ex: exn) = 
        match compileTimeErrorLog with
        | Some errorLog -> 
            errorLog.Value <- (sourceLine, sourcePath, ex) :: errorLog.Value
            fun _ -> 
                raise <| AggregateException("One or more exceptions occured when compiling queries.", 
                            errorLog.Value 
                            |> List.map (fun (line, source, ex) -> CompileTimeException($"Cannot compile query in {sourcePath}, line: {sourceLine}", ex) :> exn))
        | None ->
            raise <| CompileTimeException($"Cannot compile query in {sourcePath}, line: {sourceLine}", ex)

    new(createConnection: unit -> DbConnection) = 
        QueryBuilder(QueryConfig.Default(createConnection))

    member __.Config = config

    member __.Timeout(timeout: int) = 
        QueryBuilder({ config with Timeout = Some timeout })

    member __.LogCompileTimeErrors() = 
        QueryBuilder({ config with LogCompileTimeErrors = true })

    member __.CompileTimeErrorLog = 
        match compileTimeErrorLog with
        | Some log -> log.Value
        | None -> []

    member __.Sql (createParamSetter: IParamSetterProvider * unit -> IParamSetter<'Params>, 
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : (IRowGetterProvider * IDataReader -> IResultReader<'Result>) -> string -> 'Params -> IConnector -> Async<'Result> =         
        fun (createResultReader: IRowGetterProvider * IDataReader -> IResultReader<'Result>) (commandText: string) ->
            try
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let builderParams = provider :> GenericSetters.ISetterProvider<unit, IDbCommand>, ()                        
                let paramSetter = createParamSetter (builderParams)

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)
                let resultReader = executePrototypeQuery(CommandType.Text, commandText, paramSetter.SetArtificial, createResultReader')

                fun (parameters: 'Params) (provider: IConnector) ->
                    executeQuery(provider, commandText, resultReader, fun cmd -> paramSetter.SetValue(parameters, cmd))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    member __.Sql (createParamSetter1: IParamSetterProvider * unit -> IParamSetter<'Params1>, 
                   createParamSetter2: IParamSetterProvider * unit -> IParamSetter<'Params2>,
                   [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                   [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                   : (IRowGetterProvider * IDataReader -> IResultReader<'Result>) -> string -> 'Params1 -> 'Params2 -> IConnector -> Async<'Result> = 
        fun (createResultReader: IRowGetterProvider * IDataReader -> IResultReader<'Result>) (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let builderParams = provider :> GenericSetters.ISetterProvider<unit, IDbCommand>, ()                        
                let paramSetter1 = createParamSetter1(builderParams)
                let paramSetter2 = createParamSetter2(builderParams)

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)
                let resultReader = executePrototypeQuery(CommandType.Text, commandText, (fun cmd -> paramSetter1.SetArtificial cmd; paramSetter2.SetArtificial cmd), createResultReader')

                let setParams (parameters1: 'Params1, parameters2: 'Params2) (command: DbCommand) = 
                    paramSetter1.SetValue(parameters1, command) 
                    paramSetter2.SetValue(parameters2, command)

                fun (parameters1: 'Params1) (parameters2: 'Params2) (provider: IConnector) ->
                    executeQuery(provider, commandText, resultReader, setParams(parameters1, parameters2))
            with ex ->
                handleException(sourcePath, sourceLine, ex)

    member __.Proc (paramSetter: IParamSetterProvider * unit -> IParamSetter<'Params>,
                    [<CallerFilePath; Optional; DefaultParameterValue("")>] sourcePath: string,
                    [<CallerLineNumber; Optional; DefaultParameterValue(0)>] sourceLine: int)
                    : (IOutParamGetterProvider * unit -> IOutParamGetter<'OutParams>) -> (IRowGetterProvider * IDataReader -> IResultReader<'Result>) -> string -> 'Params -> IConnector -> Async<'Result * 'OutParams> = 
        fun (outParamGetter: IOutParamGetterProvider * unit -> IOutParamGetter<'OutParams>) (resultReaderBuilder: IRowGetterProvider * IDataReader -> IResultReader<'Result>) (commandText: string) ->
            try                        
                let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(config.ParamBuilders)
                let builderParams = provider :> GenericSetters.ISetterProvider<unit, IDbCommand>, ()                       
                let paramSetter = paramSetter(builderParams)
                        
                let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(config.OutParamBuilders)
                let builderParams = outParamProvider :> IOutParamGetterProvider, ()                        
                let outParamGetter = outParamGetter(builderParams)

                let setArtificialParams command = 
                    paramSetter.SetArtificial(command)
                    outParamGetter.Create(command)

                let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(config.RowBuilders)
                let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

                let resultReader = executePrototypeQuery(CommandType.StoredProcedure, commandText, setArtificialParams, resultReaderBuilder')
                fun (parameters: 'Params) (provider: IConnector) ->
                    executeProcedure(provider, commandText, outParamGetter, resultReader, fun cmd -> paramSetter.SetValue(parameters, cmd))
            with ex ->
                handleException(sourcePath, sourceLine, ex)
    