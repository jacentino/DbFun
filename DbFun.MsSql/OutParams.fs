namespace DbFun.MsSql.Builders

open System
open DbFun.Core
open DbFun.Core.Builders
open System.Data

module OutParams = 

    type ReturnBuilder() = 

        interface OutParamsImpl.IBuilder with
                
            member __.CanBuild(argType: Type): bool = argType = typeof<int>
                    
            member __.Build(name: string, _, ()): IOutParamGetter<'Result> = 
                { new IOutParamGetter<'Result> with
                    member __.Create (command: IDbCommand) = 
                        let param = command.CreateParameter()
                        param.ParameterName <- name
                        param.DbType <- DbType.Int32
                        param.Direction <- ParameterDirection.ReturnValue
                        command.Parameters.Add param |> ignore
                    member __.Get(command: IDbCommand): 'Result = 
                        let ordinal = command.Parameters.IndexOf(name)
                        if ordinal = -1 then
                            failwithf "Return parameter doesn't exist: %s" name
                        let param = command.Parameters.[ordinal] :?> IDataParameter
                        Convert.ChangeType(param.Value, typeof<'Result>) :?> 'Result
                    member __.IsNull(command: IDbCommand): bool = 
                        let ordinal = command.Parameters.IndexOf(name)
                        if ordinal = -1 then
                            failwithf "Return parameter doesn't exist: %s" name
                        let param = command.Parameters.[ordinal] :?> IDataParameter
                        param.Value = DBNull.Value
                }

/// <summary>
/// Provides methods creating various output parameter builders.
/// </summary>
type OutParams() =
    inherit Builders.OutParams()

    // TODO: should be possible to solve it better way
    static let returnBuilder = OutParams.ReturnBuilder() :> OutParamsImpl.IBuilder

    /// <summary>
    /// Creates return parameter builder.
    /// </summary>
    /// <param name="name">
    /// The return parameter name.
    /// </param>
    static member Return(name: string): BuildOutParamGetter<int> = 
        fun (provider, _) -> returnBuilder.Build<int>(name, provider, ())

    /// <summary>
    /// Creates builder of set of output parameters specified as 'Arg type and return parameter.
    /// </summary>
    /// <param name="retName">
    /// The retur parameter name.
    /// </param>
    /// <param name="argName">
    /// The name or record prefix of output parameters.
    /// </param>
    static member ReturnAnd<'Arg>(retName: string, ?argName: string): BuildOutParamGetter<int * 'Arg> =
        fun (provider, _) -> 
            let retp = fun (provider, ()) -> returnBuilder.Build<int>(retName, provider, ())
            let outp = OutParams.Auto(?name = argName)
            let createGetter = OutParams.Tuple<int, 'Arg>(retp, outp) 
            createGetter(provider, ())
            
        
        
