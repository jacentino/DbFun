namespace MoreSqlFun.TestTools

open System
open System.Data
open System.Data.Common
open Microsoft.Data.SqlClient

open Moq
open MoreSqlFun.Core.Builders.Queries


module Mocks = 

    let col<'t> (name: string) = name, typeof<'t>

    let createDataReaderMock (data: ((string * Type) list * (obj list) list) list): DbDataReader = 
        
        let rsIndex = ref 0
        let rowIndex = ref -1

        let currents() = 
            let header, data = data.[rsIndex.Value]
            {| header = header; data = data; row = if rowIndex.Value >= 0 then data.[rowIndex.Value] else [] |}

        let get(index: int): obj = currents().row.[index]

        { new DbDataReader() with
            member this.Close(): unit = ()
            member this.Depth: int = 
                raise (System.NotImplementedException())
            member this.FieldCount: int = 
                currents().header.Length
            member this.GetBoolean(i: int): bool = 
                get(i) |> unbox
            member this.GetByte(i: int): byte = 
                get(i) |> unbox
            member this.GetBytes(i: int, fieldOffset: int64, buffer: byte array, bufferoffset: int, length: int): int64 = 
                raise (System.NotImplementedException())
            member this.GetChar(i: int): char = 
                get(i) |> unbox
            member this.GetChars(i: int, fieldoffset: int64, buffer: char array, bufferoffset: int, length: int): int64 = 
                raise (System.NotImplementedException())
            member this.GetDataTypeName(i: int): string = 
                raise (System.NotImplementedException())
            member this.GetDateTime(i: int): DateTime = 
                get(i) |> unbox
            member this.GetDecimal(i: int): decimal = 
                get(i) |> unbox
            member this.GetDouble(i: int): float = 
                get(i) |> unbox
            member this.GetFieldType(i: int): Type = 
                snd <| currents().header.[i]
            member this.GetFloat(i: int): float32 = 
                get(i) |> unbox
            member this.GetGuid(i: int): Guid = 
                get(i) |> unbox
            member this.GetInt16(i: int): int16 = 
                get(i) |> unbox
            member this.GetInt32(i: int): int = 
                get(i) |> unbox
            member this.GetInt64(i: int): int64 = 
                get(i) |> unbox
            member this.GetName(i: int): string = 
                fst <| currents().header.[i]
            member this.GetOrdinal(name: string): int = 
                currents().header |> List.findIndex (fst >> (=) name)
            member this.GetSchemaTable(): DataTable = 
                raise (System.NotImplementedException())
            member this.GetString(i: int): string = 
                get(i) |> unbox
            member this.GetValue(i: int): obj = 
                get(i) 
            member this.GetValues(values: obj array): int = 
                raise (System.NotImplementedException())
            member this.IsClosed: bool = 
                raise (System.NotImplementedException())
            member this.IsDBNull(i: int): bool = 
                get(i) = null
            member this.Item
                with get (i: int): obj = 
                    raise (System.NotImplementedException())
            member this.Item
                with get (name: string): obj = 
                    raise (System.NotImplementedException())
            member this.NextResult(): bool = 
                if rsIndex.Value < data.Length - 1 then
                    rsIndex.Value <- rsIndex.Value + 1
                    rowIndex.Value <- -1
                    true
                else
                    false
            member this.Read(): bool = 
                if rsIndex.Value < data.Length && rowIndex.Value < currents().data.Length - 1 then
                    rowIndex.Value <- rowIndex.Value + 1
                    true
                else
                    false
            member this.RecordsAffected: int = 
                  raise (System.NotImplementedException())
            member this.HasRows: bool = 
                  raise (System.NotImplementedException())
            member this.GetEnumerator(): Collections.IEnumerator = 
                  raise (System.NotImplementedException())
        }

    let createPrototypeAndRegular data = 
        createDataReaderMock(data), createDataReaderMock(data)

    let createCommandExecutorMock (args: (string * DbType) list) data = 

        let checkParams (command: DbCommand) = 
            for name, dbType in args do
                let param = command.Parameters.[name]
                if param.DbType <> dbType then
                    raise (ArgumentException(sprintf "Parameter %s has incorrect type. Expected: %A, actual: %A " name dbType param.DbType))

        { new ICommandExecutor with
            member __.OpenConnection(_: DbConnection): unit = 
                ()
            member __.CreateCommand(connection: DbConnection): DbCommand = 
                connection.CreateCommand()
            member __.Execute(command: DbCommand, _: CommandBehavior): IDataReader = 
                checkParams(command)
                createDataReaderMock(data)
            member __.ExecuteAsync(command: DbCommand, _: CommandBehavior): Async<IDataReader> = 
                checkParams(command)
                async.Return (createDataReaderMock(data))
        }

    let setupCommandOutParams (data: (string * obj) seq) = 
        { new ICommandExecutor with
            member __.OpenConnection(_: DbConnection): unit = 
                ()
            member __.CreateCommand(connection: DbConnection): DbCommand = 
                connection.CreateCommand()
            member __.Execute(command: DbCommand, _: CommandBehavior): IDataReader = 
                for name, value in data do
                    command.Parameters.[name].Value <- value
                createDataReaderMock []
            member __.ExecuteAsync(command: DbCommand, _: CommandBehavior): Async<IDataReader> = 
                for name, value in data do
                    command.Parameters.[name].Value <- value
                async.Return (createDataReaderMock [])
        }


    let createConnectionMock data = 
        let connection = Mock<IDbConnection>()
        let command = Mock<IDbCommand>()
        command.Setup(fun x -> x.CreateParameter()).Returns(SqlParameter()) |> ignore
        command.SetupGet(fun x -> x.Parameters).Returns(Mock<IDataParameterCollection>().Object) |> ignore
        command.Setup(fun x -> x.ExecuteReader()).Returns(createDataReaderMock data) |> ignore
        connection.Setup(fun x -> x.CreateCommand()).Returns(command.Object) |> ignore
        connection.Object
