namespace DbFun.Core

open System

module ComputationBuilderImpl = 
    
    /// <summary>
    /// The computation builder for database computations.
    /// </summary>
    type DbSessionBuilder() = 
        member __.Return(value: 't): DbCall<'t> = fun _ -> async { return value }
        member __.ReturnFrom(value: DbCall<'t>): DbCall<'t> = value
        member __.Bind(rd: DbCall<'t1>, f: 't1 -> DbCall<'t2>): DbCall<'t2> = 
            fun env -> async {
                    let! value = rd env
                    return! (f value) env
                }                    
        member __.Zero(_) = fun _ -> async { return () }
        member this.Combine(value: DbCall<'t1>, y: DbCall<'t2>): DbCall<'t2> = this.Bind(value, fun value' -> y)
        member __.Delay(f: unit-> 'Env -> 't Async) = fun env -> async { return! f () env }
        member __.For (items: seq<'t>,  f: 't -> DbCall<unit>): DbCall<unit> = 
            fun env -> async {
                for item in items do 
                    do! f item env
            }
        member __.Using(value: 't, f: 't -> DbCall<'u> when 't :> IDisposable) =
            fun env -> async {
                try
                    return! f value env
                finally
                    value.Dispose()
            }
    
[<AutoOpen>]
module ComputationBuilder =

    /// <summary>
    /// Builds database workflow using computation expressions syntax.
    /// </summary>
    let dbsession = ComputationBuilderImpl.DbSessionBuilder()


