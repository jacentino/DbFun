namespace DbFun.MsSql.Builders

open System
open DbFun.Core
open DbFun.Core.Builders
open Microsoft.Data.SqlClient.Server
open System.Linq.Expressions

type ITVParamSetter<'Arg> = GenericSetters.ISetter<SqlDataRecord, 'Arg>

type ITVParamSetterProvider = GenericSetters.ISetterProvider<SqlDataRecord, SqlDataRecord>

type TVParamSpecifier<'Arg> = ITVParamSetterProvider * SqlDataRecord -> ITVParamSetter<'Arg>

module TableValuedParamsImpl = 

    type IBuilder = GenericSetters.IBuilder<SqlDataRecord, SqlDataRecord>

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

            member this.Build<'Arg> (name: string, _, prototype: SqlDataRecord) = 
                let ordinal = prototype.GetOrdinal(name)
                let fieldType = prototype.GetFieldType(ordinal)
                let colSetter = typedColAccessMethods |> List.tryFind (fst >> (=) fieldType) |> Option.map snd |> Option.defaultValue setValueMethod
                let recParam = Expression.Parameter(typeof<SqlDataRecord>)
                let valueParam = Expression.Parameter(typeof<'Arg>)
                let convertedValue = 
                    if typeof<'Arg> = typeof<char> then
                        Expression.New(typeof<string>.GetConstructor([| typeof<char>; typeof<int> |]), valueParam, Expression.Constant(1)) :> Expression
                    elif typeof<'Arg> <> fieldType then
                        try
                            Expression.Convert(valueParam, fieldType) :> Expression
                        with :? InvalidOperationException as ex ->
                            raise <| Exception(sprintf "Column type doesn't match field type: %s (%s -> %s)" name valueParam.Type.Name fieldType.Name, ex)
                    else
                        valueParam :> Expression
                let call = Expression.Call(recParam, colSetter, Expression.Constant(ordinal), convertedValue)
                let setter = Expression.Lambda<Action<SqlDataRecord, 'Arg>>(call, recParam, valueParam).Compile()
                { new ITVParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, index: int option, command: SqlDataRecord) = 
                        setter.Invoke(command, value)
                    member __.SetNull(index: int option, command: SqlDataRecord) = 
                        command.SetDBNull(ordinal)
                    member __.SetArtificial(index: int option, command: SqlDataRecord) = 
                        command.SetValue(ordinal, this.GetArtificialValue<'Arg>())
                }

    let getDefaultBuilders(): IBuilder list = 
        SimpleBuilder() :: GenericSetters.getDefaultBuilders()

/// <summary>
/// Provides methods creating various table-valued parameter builders.
/// </summary>
type TVParams() = 
    inherit GenericSetters.GenericSetterBuilder<SqlDataRecord, SqlDataRecord>()
           
/// <summary>
/// The field-to-column of SqlDataRecord mapping override.
/// </summary>
type TVParamOverride<'Arg> = GenericSetters.Override<SqlDataRecord, SqlDataRecord, 'Arg>

