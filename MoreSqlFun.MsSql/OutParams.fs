namespace MoreSqlFun.MsSql.Builders

open System
open MoreSqlFun.Core
open MoreSqlFun.Core.Builders
open System.Data

module OutParams = 

    type ReturnBuilder() = 

        interface OutParams.IBuilder with
                
            member __.CanBuild(argType: Type): bool = argType = typeof<int>
                    
            member __.Build(_, name: string) (): IOutParamGetter<'Result> = 
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

type OutParamBuilder(builders: OutParams.IBuilder seq) =
    inherit Builders.OutParamBuilder(builders)

    let returnBuilder = OutParams.ReturnBuilder() :> OutParams.IBuilder

    member this.Return(name: string): unit -> IOutParamGetter<int> = 
        returnBuilder.Build<int>(this, name)

    member this.ReturnAnd<'Arg>(retName: string, argName: string): unit -> IOutParamGetter<int * 'Arg> =
        this.Tuple<int, 'Arg>(returnBuilder.Build<int>(this, retName), this.Simple<'Arg>(argName))
        
        
