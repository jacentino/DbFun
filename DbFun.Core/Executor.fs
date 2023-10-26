namespace DbFun.Core

open System.Data
open System.Data.Common

module Executor = 

    let executeReaderAsync (command: IDbCommand, behavior: CommandBehavior): Async<IDataReader> = 
        async {
            match command with 
            | :? DbCommand as dbCommand ->
                let! token = Async.CancellationToken
                let! reader = dbCommand.ExecuteReaderAsync(behavior, token) |> Async.AwaitTask
                return reader
            | _ ->
                let reader = command.ExecuteReader(behavior) 
                return reader
        }
        
    let nextResultAsync(reader: IDataReader): Async<bool> = 
        async {
            match reader with 
            | :? DbDataReader as dbReader ->
                let! token = Async.CancellationToken
                let! result = dbReader.NextResultAsync(token) |> Async.AwaitTask
                return result
            | _ ->
                let reader = reader.NextResult()
                return reader
        }


