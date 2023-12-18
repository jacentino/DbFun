namespace DbFun.Core

open System.Data

/// <summary>
/// Represents asynchronous database computation, that needs an open connection (provided by IConnector object) to be performed.
/// </summary>
/// <remarks>
/// DbCall can be considered as a combination of Async and Reader monads and is used such a way by dbsession computation expressions.
/// </remarks>
type DbCall<'Result> = IConnector -> Async<'Result>

type DbCall() = 

    static let wrapInTransaction (isolationLevel: IsolationLevel option) (f: IConnector -> Async<'T>) (con: IConnector) =
        async {
            if con.Transaction = null then
                use transaction = 
                    isolationLevel 
                    |> Option.map con.Connection.BeginTransaction 
                    |> Option.defaultWith con.Connection.BeginTransaction
                let! value = f(con.With transaction)
                transaction.Commit()
                return value
            else
                return! f(con)
        }

    /// <summary>
    /// Transforms DbCall to another DbCall.
    /// </summary>
    /// <param name="f">
    /// Function performing transformation.
    /// </param>
    /// <param name="dbCall">
    /// The source dbCall value.
    /// </param>
    static member Map (f: 'T -> 'U) (dbCall: DbCall<'T>): DbCall<'U> = 
        fun con ->
            async {
                let! value = dbCall(con)
                return f(value)
            }

    /// <summary>
    /// Catches exception that occured inside dbsession workflow and returns Choice object containing either 
    /// regular result or exception.
    /// </summary>
    /// <param name="dbCall">
    /// The source dbCall value.
    /// </param>
    static member Catch (dbCall: DbCall<'T>): DbCall<Choice<'T, exn>> = 
        fun (con: IConnector) ->
            async { 
                return! dbCall(con)
            } |> Async.Catch

    /// <summary>
    /// Lifts value wrapped in Async to DbCall.
    /// </summary>
    /// <param name="a">
    /// The async value.
    /// </param>
    static member FromAsync<'T> (a: Async<'T>) (_: IConnector) = a
        
    /// <summary>
    /// Wraps database computation in a transaction.
    /// </summary>
    /// <param name="dbCall">
    /// The source dbCall value.
    /// </param>
    static member InTransaction (dbCall: DbCall<'T>): DbCall<'T> = 
        wrapInTransaction None dbCall

    /// <summary>
    /// Wraps database computation in a transaction of a certain isolation level.
    /// </summary>
    /// <param name="isolationLevel">
    /// The transaction isolation level.
    /// </param>
    static member InTransactionWith (isolationLevel: IsolationLevel): DbCall<'T> -> DbCall<'T> = 
        wrapInTransaction (Some isolationLevel)

    /// <summary>
    /// Creates a connection and executes a database computations on it. Disposes the connection when done.
    /// </summary>
    /// <param name="createConnection">
    /// Creates a connection.
    /// </param>
    /// <param name="dbCall">
    /// Database computations to be executed.
    /// </param>
    static member Run (createConnection: unit-> IDbConnection, dbCall: DbCall<'Result>): Async<'Result> = 
        async {
            use connection = createConnection()
            connection.Open()
            let connector = Connector(connection)
            return! dbCall(connector)
        }


module List =
    
    /// <summary>
    /// Transforms list of DbCall values to DbCall of list.
    /// </summary>
    /// <param name="items">
    /// The list of DbCall values.
    /// </param>
    /// <param name="con">
    /// The connector.
    /// </param>
    let toDbCall (items: DbCall<'t> list) (con: IConnector): 't list Async = async {
        let mutable result = []
        for dbCall in items do
            let! item = dbCall con
            result <- item :: result
        return result |> List.rev   
    }

module Array =
    
    /// <summary>
    /// Transforms array of DbCall values to DbCall of array.
    /// </summary>
    /// <param name="items">
    /// The array of DbCall values.
    /// </param>
    /// <param name="con">
    /// The connector.
    /// </param>
    let toDbCall (items: DbCall<'t> array) (con: IConnector): 't array Async = async {
        let result = Array.zeroCreate items.Length
        for i in 0..items.Length - 1 do
            let! item = (items[i]) con
            result.[i] <- item
        return result 
    }

module Option =

    /// <summary>
    /// Transforms option of DbCall value to DbCall of option.
    /// </summary>
    /// <param name="value">
    /// The option of DbCall value.
    /// </param>
    /// <param name="con">
    /// The connector.
    /// </param>
    let toDbCall (value: DbCall<'t> option) (con: IConnector): 't option Async = async {
        match value with
        | Some dbCall ->
            let! value = dbCall con
            return Some value
        | None ->
            return None
    }


