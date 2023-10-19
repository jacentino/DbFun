namespace MoreSqlFun.TestTools

open System
open System.Data
open Microsoft.Data.SqlClient

open Moq


module Mocks = 

    let col<'t> (name: string) = name, typeof<'t>

    let createDataReaderMock (data: ((string * Type) list * (obj list) list) list): IDataReader = 
        
        let rsIndex = ref 0
        let rowIndex = ref -1

        let currents() = 
            let header, data = data.[rsIndex.Value]
            {| header = header; data = data; row = if rowIndex.Value >= 0 then data.[rowIndex.Value] else [] |}

        let get(index: int): obj = currents().row.[index]

        { new IDataReader with
            member this.Dispose(): unit = 
                ()
            member this.GetData(i: int): IDataReader = 
                raise (System.NotImplementedException())
            member this.Item
                with get (i: int): obj = 
                    raise (System.NotImplementedException())
            member this.Item
                with get (name: string): obj = 
                    raise (System.NotImplementedException())
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
        }

    let createPrototypeAndRegular data = 
        createDataReaderMock(data), createDataReaderMock(data)

    let createMockParameterCollection() = 
        let container = System.Collections.ArrayList()
        { new IDataParameterCollection with
              member this.Add(value: obj): int = container.Add(value)
              member this.Clear(): unit = container.Clear()
              member this.Contains(parameterName: string): bool = this.IndexOf(parameterName) <> -1
              member this.Contains(value: obj): bool = container.Contains(value)
              member this.CopyTo(array: Array, index: int): unit = container.CopyTo(array, index)
              member this.Count: int = container.Count
              member this.GetEnumerator(): Collections.IEnumerator = container.GetEnumerator()                  
              member this.IndexOf(parameterName: string): int = 
                    let mutable found = -1
                    for i in 0..container.Count - 1 do
                        if (container.[i] :?> IDataParameter).ParameterName = parameterName then
                            found <- i
                    found
              member this.IndexOf(value: obj): int = container.IndexOf(value)
              member this.Insert(index: int, value: obj): unit = container.Insert(index, value)
              member this.IsFixedSize: bool = container.IsFixedSize
              member this.IsReadOnly: bool = container.IsReadOnly
              member this.IsSynchronized: bool = container.IsSynchronized
              member this.Item
                  with get (parameterName: string): obj = container.[this.IndexOf(parameterName)]
                  and set (parameterName: string) (v: obj): unit = container.[this.IndexOf(parameterName)] <- v
              member this.Item
                  with get (index: int): obj = container.[index]
                  and set (index: int) (v: obj): unit = container.[index] <- v
              member this.Remove(value: obj): unit = container.Remove(value)
              member this.RemoveAt(parameterName: string): unit = container.RemoveAt(this.IndexOf(parameterName))
              member this.RemoveAt(index: int): unit = container.RemoveAt(index)
              member this.SyncRoot: obj = container.SyncRoot                  
        }

    let setupCommandOutParams (data: (string * obj) seq) = 
        let connection = Mock<IDbConnection>()
        let command = Mock<IDbCommand>()
        command.Setup(fun x -> x.CreateParameter()).Returns(Func<IDbDataParameter>(fun () -> SqlParameter())) |> ignore
        let parameters = createMockParameterCollection()
        command.SetupGet(fun x -> x.Parameters).Returns(parameters) |> ignore
        command
            .Setup(fun x -> x.ExecuteReader(It.IsAny<CommandBehavior>())).Returns(createDataReaderMock []) 
            .Callback(fun () -> 
                for name, value in data do
                    (parameters.[name] :?> IDataParameter).Value <- value)
            |> ignore
        connection.Setup(fun x -> x.CreateCommand()).Returns(command.Object) |> ignore
        connection.Object

    let createConnectionMock (args: (string * DbType) list) data = 
        let connection = Mock<IDbConnection>()
        let command = Mock<IDbCommand>()
        command.Setup(fun x -> x.CreateParameter()).Returns(Func<IDbDataParameter>(fun () -> SqlParameter())) |> ignore
        let parameters = createMockParameterCollection()
        command.SetupGet(fun x -> x.Parameters).Returns(parameters) |> ignore
        command
            .Setup(fun x -> x.ExecuteReader(It.IsAny<CommandBehavior>()))
            .Callback(fun () ->
                for name, dbType in args do
                    let param = parameters.[name] :?> IDataParameter
                    if param.DbType <> dbType then
                        raise (ArgumentException(sprintf "Parameter %s has incorrect type. Expected: %A, actual: %A " name dbType param.DbType))
            )
            .Returns(createDataReaderMock data) 
        |> ignore
        command.Setup(fun x -> x.ExecuteReader()).Returns(createDataReaderMock data) |> ignore
        connection.Setup(fun x -> x.CreateCommand()).Returns(command.Object) |> ignore
        connection.Object
