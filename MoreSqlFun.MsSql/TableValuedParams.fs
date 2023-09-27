namespace MoreSqlFun.MsSql.Builders

open System
open System.Data 
open MoreSqlFun.Core
open MoreSqlFun.Core.Builders
open Microsoft.Data.SqlClient.Server
open System.Linq.Expressions

type ITVParamSetter<'Arg> = GenericSetters.ISetter<SqlDataRecord, 'Arg>

type ITVParamSetterProvider = GenericSetters.ISetterProvider<IDbCommand>

module TableValuedParams = 

    type IBuilder = GenericSetters.IBuilder<SqlDataRecord>

    type SimpleBuilder() =

        static let typedColAccessMethods = 
            [
                typeof<Boolean>,    "SetBoolean"
                typeof<Byte>,       "SetByte"
                typeof<Char>,       "SetChar"
                typeof<DateTime>,   "SetDateTime"
                typeof<Decimal>,    "SetDecimal"
                typeof<Double>,     "SetDouble"
                typeof<float>,      "SetFloat"
                typeof<Guid>,       "SetGuid"
                typeof<Int16>,      "SetInt16"
                typeof<Int32>,      "SetInt32"
                typeof<Int64>,      "SetInt64" 
                typeof<string>,     "SetString"
            ] 
            |> List.map (fun (t, name) -> t, typeof<SqlDataRecord>.GetMethod(name))                

        static let setValueMethod = typeof<SqlDataRecord>.GetMethod("SetValue")


        member __.GetArtificialValue<'Type>(): obj = 
            if typeof<'Type> = typeof<string> then box ""
            elif typeof<'Type> = typeof<DateTime> then box DateTime.Now
            elif typeof<'Type> = typeof<byte[]> then box [||]
            elif typeof<'Type>.IsClass then null
            else box Unchecked.defaultof<'Type>

        interface IBuilder with

            member __.CanBuild (argType: Type) = Types.isSimpleType(argType)

            member this.Build<'Arg> (_, name: string) = 
                let ordinal = 0
                let fieldType = typeof<'Arg>
                let colSetter = typedColAccessMethods |> List.tryFind (fst >> (=) fieldType) |> Option.map snd |> Option.defaultValue setValueMethod
                let recParam = Expression.Parameter(typeof<SqlDataRecord>)
                let valueParam = Expression.Parameter(typeof<'Arg>)
                let convertedValue = 
                    if typeof<'Arg> <> fieldType then
                        try
                            Expression.Convert(valueParam, fieldType) :> Expression
                        with :? InvalidOperationException as ex ->
                            raise <| Exception(sprintf "Column type doesn't match field type: %s (%s -> %s)" name valueParam.Type.Name typeof<'Arg>.Name, ex)
                    else
                        valueParam :> Expression
                let call = Expression.Call(recParam, colSetter, Expression.Constant(ordinal), convertedValue)
                let setter = Expression.Lambda<Action<SqlDataRecord, 'Arg>>(call, recParam, valueParam).Compile()
                { new ITVParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, command: SqlDataRecord) = 
                        setter.Invoke(command, value)
                    member __.SetNull(command: SqlDataRecord) = 
                        command.SetDBNull(ordinal)
                    member __.SetArtificial(command: SqlDataRecord) = 
                        command.SetValue(ordinal, this.GetArtificialValue<'Arg>())
                }


open TableValuedParams

type TVParamBuilder(builders: IBuilder seq) = 
    inherit GenericSetters.GenericSetterBuilder<SqlDataRecord>(Seq.append builders [])

    

