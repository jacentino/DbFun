namespace MoreSqlFun.Core.Builders

open System
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

type QueryBuilder(createConnection: unit -> DbConnection, ?executor: Queries.ICommandExecutor) = 

    let executor = defaultArg executor (Queries.DefaultExecutor())
        
    let createResultReader(commandType: CommandType, commandText: string, resultReaderBuilder: IDataReader -> IResultReader<'Result>, setParams: DbCommand -> unit) =
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

    member __.Sql (paramSetter: unit -> IParamSetter<'Params>): (IDataReader -> IResultReader<'Result>) -> string -> 'Params -> IConnector -> Async<'Result> = 
        fun (resultReaderBuilder: IDataReader -> IResultReader<'Result>) (commandText: string) ->
            let paramSetter = paramSetter()
            let resultReader = createResultReader(CommandType.Text, commandText, resultReaderBuilder, paramSetter.SetArtificial)
            fun (parameters: 'Params) (provider: IConnector) ->
                executeQuery(provider, commandText, resultReader, fun cmd -> paramSetter.SetValue(parameters, cmd))

    member __.Sql (paramSetter1: unit -> IParamSetter<'Params1>, paramSetter2: unit -> IParamSetter<'Params2>): (IDataReader -> IResultReader<'Result>) -> string -> 'Params1 -> 'Params2 -> IConnector -> Async<'Result> = 
        fun (resultReaderBuilder: IDataReader -> IResultReader<'Result>) (commandText: string) ->
            let paramSetter1 = paramSetter1()
            let paramSetter2 = paramSetter2()
            let resultReader = createResultReader(CommandType.Text, commandText, resultReaderBuilder, fun cmd -> paramSetter1.SetArtificial cmd; paramSetter2.SetArtificial cmd)
            fun (parameters1: 'Params1) (parameters2: 'Params2) (provider: IConnector) ->
                executeQuery(provider, commandText, resultReader, fun cmd -> paramSetter1.SetValue(parameters1, cmd); paramSetter2.SetValue(parameters2, cmd))

    member __.Proc (paramSetter: unit -> IParamSetter<'Params>): (unit -> IOutParamGetter<'OutParams>) -> (IDataReader -> IResultReader<'Result>) -> string -> 'Params -> IConnector -> Async<'Result * 'OutParams> = 
        fun (outParamGetter: unit -> IOutParamGetter<'OutParams>) (resultReaderBuilder: IDataReader -> IResultReader<'Result>) (commandText: string) ->
            let paramSetter = paramSetter()
            let outParamGetter = outParamGetter()
            let setParams command = 
                paramSetter.SetArtificial(command)
                outParamGetter.Create(command)
            let resultReader = createResultReader(CommandType.StoredProcedure, commandText, resultReaderBuilder, setParams)
            fun (parameters: 'Params) (provider: IConnector) ->
                executeProcedure(provider, commandText, outParamGetter, resultReader, fun cmd -> paramSetter.SetValue(parameters, cmd))
