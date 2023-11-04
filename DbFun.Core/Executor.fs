namespace DbFun.Core

open System.Data
open System.Data.Common

/// <summary>
/// Helper module executing various async functions on a connection/command/data reader supporting async, or its
/// sync counterparts for objects, that don't support async.
/// </summary>
module Executor = 

    /// <summary>
    /// Executes reader on a command.
    /// </summary>
    /// <param name="command">The command.</param>
    /// <param name="behavior">The command behavior.</param>
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
        
    /// <summary>
    /// Moves data reader to next result.
    /// </summary>
    /// <param name="reader"> The data reader.</param>
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


