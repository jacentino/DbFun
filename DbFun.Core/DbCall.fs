namespace DbFun.Core

open System.Data

/// <summary>
/// Represents asynchronous database computation, that needs an open connection (provided by IConnector object) to be performed.
/// </summary>
/// <remarks>
/// DbCall can be considered as a combination of Async and Reader monads and is used such a way by dbsession computation expressions.
/// </remarks>
type DbCall<'DbKey, 'Result> = IConnector<'DbKey> -> Async<'Result>

type DbCall<'Result> = DbCall<unit, 'Result>

type DbCall() = 

    static let wrapInTransaction (dbKey: 'DbKey, isolationLevel: IsolationLevel option) (f: IConnector<'DbKey> -> Async<'T>) (con: IConnector<'DbKey>) =
        async {
            if con.GetTransaction(dbKey) = null then
                use transaction = 
                    isolationLevel 
                    |> Option.map (con.GetConnection(dbKey).BeginTransaction)
                    |> Option.defaultWith (con.GetConnection(dbKey).BeginTransaction)
                let! value = f(con.With(dbKey, transaction))
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
    static member Map (f: 'T -> 'U) (dbCall: DbCall<'DbKey, 'T>): DbCall<'DbKey, 'U> = 
        fun (con : IConnector<'DbKey>) ->
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
    static member Catch (dbCall: DbCall<'DbKey, 'T>): DbCall<'DbKey, Choice<'T, exn>> = 
        fun (con: IConnector<'DbKey>) ->
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
    static member InTransaction (dbCall: DbCall<unit, 'T>): DbCall<unit, 'T> = 
        wrapInTransaction ((), None) dbCall

    /// <summary>
    /// Wraps database computation in a transaction of a certain isolation level.
    /// </summary>
    /// <param name="isolationLevel">
    /// The transaction isolation level.
    /// </param>
    static member InTransaction (isolationLevel: IsolationLevel): DbCall<unit, 'T> -> DbCall<unit, 'T> = 
        wrapInTransaction ((), Some isolationLevel)

    /// <summary>
    /// Wraps database computation in a transaction of a certain isolation level.
    /// </summary>
    /// <param name="dbKey">
    /// Value determining a database.
    /// </param>
    /// <param name="isolationLevel">
    /// The transaction isolation level.
    /// </param>
    static member InTransaction (dbKey: 'DbKey, ?isolationLevel: IsolationLevel): DbCall<'DbKey, 'T> -> DbCall<'DbKey, 'T> = 
        wrapInTransaction (dbKey, isolationLevel)

    /// <summary>
    /// Creates a connection and executes a database computations on it. Disposes the connection when done.
    /// </summary>
    /// <param name="createConnection">
    /// Creates a connection.
    /// </param>
    /// <param name="dbCall">
    /// Database computations to be executed.
    /// </param>
    static member Run (createConnection: 'DbKey-> IDbConnection, dbCall: DbCall<'DbKey, 'Result>): Async<'Result> = 
        async {
            use connector = new Connector<'DbKey>(createConnection)
            return! dbCall(connector)
        }

    /// <summary>
    /// Executes many database computations in parallel.
    /// </summary>
    /// <param name="dbCalls">
    /// Database computations to be executed in parallel.
    /// </param>
    /// <param name="connector">
    /// The database connector.
    /// </param>
    static member Parallel (dbCalls: DbCall<'DbKey, 'Result> seq) (connector: IConnector<'DbKey>): Async<'Result array> = 
        Async.Parallel
            [ for call in dbCalls do 
                async {
                    use clone = connector.Clone()
                    return! call(clone)                
                }
            ]


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
    let toDbCall (items: DbCall<'DbKey, 't> list) (con: IConnector<'DbKey>): 't list Async = async {
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
    let toDbCall (items: DbCall<'DbKey, 't> array) (con: IConnector<'DbKey>): 't array Async = async {
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
    let toDbCall (value: DbCall<'DbKey, 't> option) (con: IConnector<'DbKey>): 't option Async = async {
        match value with
        | Some dbCall ->
            let! value = dbCall con
            return Some value
        | None ->
            return None
    }


