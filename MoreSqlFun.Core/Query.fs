namespace MoreSqlFun.Core.Builders

open System.Data
open System.Data.Common
open MoreSqlFun.Core

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

type QueryBuilder(
    createConnection    : unit -> DbConnection, 
    ?executor           : Queries.ICommandExecutor,
    ?paramBuilders      : ParamsImpl.IBuilder list,
    ?outParamBuilders   : OutParamsImpl.IBuilder list,
    ?rowBuilders        : RowsImpl.IBuilder list,
    ?timeout            : int) =

    let executor = defaultArg executor (Queries.DefaultExecutor())
    let paramBuilders = defaultArg paramBuilders (ParamsImpl.getDefaultBuilders())
    let outParamBuilders = defaultArg outParamBuilders (OutParamsImpl.getDefaultBuilders())
    let rowBuilders = defaultArg rowBuilders (RowsImpl.getDefaultBuilders())
        
    let executePrototypeQuery(commandType: CommandType, commandText: string, setParams: DbCommand -> unit, resultReaderBuilder: IDataReader -> IResultReader<'Result>) =
        use connection = createConnection()
        executor.OpenConnection(connection)
        use command = connection.CreateCommand()
        command.CommandType <- commandType
        command.CommandText <- commandText
        setParams(command)
        use prototype = executor.Execute(command, CommandBehavior.SchemaOnly)
        resultReaderBuilder(prototype)

    let executeQuery (provider: IConnector, commandText: string, resultReader: IResultReader<'Result>, setParams: DbCommand -> unit) = 
        async {
            use command = executor.CreateCommand(provider.Connection)
            command.CommandType <- CommandType.Text
            command.CommandText <- commandText
            command.Transaction <- provider.Transaction
            match timeout with
            | Some timeout -> command.CommandTimeout <- timeout
            | None -> ()
            setParams(command)
            use! dataReader = executor.ExecuteAsync(command, CommandBehavior.Default)
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
            use! dataReader = executor.ExecuteAsync(command, CommandBehavior.Default) 
            return resultReader.Read(dataReader), outParamGetter.Get(command)
        }

    member __.Executor           = executor
    member __.ParamBuilders      = paramBuilders
    member __.OutParamBuilders   = outParamBuilders
    member __.RowBuilders        = rowBuilders
    member __.Timeout            = timeout

    member this.Configure(
            ?paramBuilders      : ParamsImpl.IBuilder list,
            ?outParamBuilders   : OutParamsImpl.IBuilder list,
            ?rowBuilders        : RowsImpl.IBuilder list,
            ?timeout            : int) = 
        QueryBuilder(
            createConnection,
            executor,
            paramBuilders       = defaultArg paramBuilders this.ParamBuilders,
            outParamBuilders    = defaultArg outParamBuilders this.OutParamBuilders,
            rowBuilders         = defaultArg rowBuilders this.RowBuilders,
            ?timeout            = (timeout |> Option.orElse timeout))

    member __.Sql (createParamSetter: IParamSetterProvider * unit -> IParamSetter<'Params>): (IRowGetterProvider * IDataReader -> IResultReader<'Result>) -> string -> 'Params -> IConnector -> Async<'Result> = 
        fun (createResultReader: IRowGetterProvider * IDataReader -> IResultReader<'Result>) (commandText: string) ->
                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(paramBuilders)
            let builderParams = provider :> GenericSetters.ISetterProvider<unit, IDbCommand>, ()                        
            let paramSetter = createParamSetter (builderParams)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(rowBuilders)
            let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)
            let resultReader = executePrototypeQuery(CommandType.Text, commandText, paramSetter.SetArtificial, createResultReader')

            fun (parameters: 'Params) (provider: IConnector) ->
                executeQuery(provider, commandText, resultReader, fun cmd -> paramSetter.SetValue(parameters, cmd))

    member __.Sql (createParamSetter1: IParamSetterProvider * unit -> IParamSetter<'Params1>, createParamSetter2: IParamSetterProvider * unit -> IParamSetter<'Params2>): (IRowGetterProvider * IDataReader -> IResultReader<'Result>) -> string -> 'Params1 -> 'Params2 -> IConnector -> Async<'Result> = 
        fun (createResultReader: IRowGetterProvider * IDataReader -> IResultReader<'Result>) (commandText: string) ->
                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(paramBuilders)
            let builderParams = provider :> GenericSetters.ISetterProvider<unit, IDbCommand>, ()                        
            let paramSetter1 = createParamSetter1(builderParams)
            let paramSetter2 = createParamSetter2(builderParams)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(rowBuilders)
            let createResultReader' prototype = createResultReader(rowGetterProvider, prototype)
            let resultReader = executePrototypeQuery(CommandType.Text, commandText, (fun cmd -> paramSetter1.SetArtificial cmd; paramSetter2.SetArtificial cmd), createResultReader')

            let setParams (parameters1: 'Params1, parameters2: 'Params2) (command: DbCommand) = 
                paramSetter1.SetValue(parameters1, command) 
                paramSetter2.SetValue(parameters2, command)

            fun (parameters1: 'Params1) (parameters2: 'Params2) (provider: IConnector) ->
                executeQuery(provider, commandText, resultReader, setParams(parameters1, parameters2))

    member __.Proc (paramSetter: IParamSetterProvider * unit -> IParamSetter<'Params>): (IOutParamGetterProvider * unit -> IOutParamGetter<'OutParams>) -> (IRowGetterProvider * IDataReader -> IResultReader<'Result>) -> string -> 'Params -> IConnector -> Async<'Result * 'OutParams> = 
        fun (outParamGetter: IOutParamGetterProvider * unit -> IOutParamGetter<'OutParams>) (resultReaderBuilder: IRowGetterProvider * IDataReader -> IResultReader<'Result>) (commandText: string) ->
                        
            let provider = GenericSetters.BaseSetterProvider<unit, IDbCommand>(paramBuilders)
            let builderParams = provider :> GenericSetters.ISetterProvider<unit, IDbCommand>, ()                       
            let paramSetter = paramSetter(builderParams)
                        
            let outParamProvider = GenericGetters.BaseGetterProvider<unit, IDbCommand>(outParamBuilders)
            let builderParams = outParamProvider :> IOutParamGetterProvider, ()                        
            let outParamGetter = outParamGetter(builderParams)

            let setArtificialParams command = 
                paramSetter.SetArtificial(command)
                outParamGetter.Create(command)

            let rowGetterProvider = GenericGetters.BaseGetterProvider<IDataRecord, IDataRecord>(rowBuilders)
            let resultReaderBuilder' prototype = resultReaderBuilder(rowGetterProvider, prototype)

            let resultReader = executePrototypeQuery(CommandType.StoredProcedure, commandText, setArtificialParams, resultReaderBuilder')
            fun (parameters: 'Params) (provider: IConnector) ->
                executeProcedure(provider, commandText, outParamGetter, resultReader, fun cmd -> paramSetter.SetValue(parameters, cmd))
