namespace MoreSqlFun.Core.Builders

open System
open System.Data
open System.Linq.Expressions
open MoreSqlFun.Core

type IRowGetter<'Result> = GenericGetters.IGetter<IDataRecord, 'Result>

type IRowGetterProvider = GenericGetters.IGetterProvider<IDataRecord, IDataRecord>

module Rows = 

    type IBuilder = GenericGetters.IBuilder<IDataRecord, IDataRecord>


    type SimpleColumnBuilder() = 

        static let typedColAccessMethods = 
            [
                typeof<Boolean>,    "GetBoolean"
                typeof<Byte>,       "GetByte"
                typeof<Char>,       "GetChar"
                typeof<DateTime>,   "GetDateTime"
                typeof<Decimal>,    "GetDecimal"
                typeof<Double>,     "GetDouble"
                typeof<float>,      "GetFloat"
                typeof<Guid>,       "GetGuid"
                typeof<Int16>,      "GetInt16"
                typeof<Int32>,      "GetInt32"
                typeof<Int64>,      "GetInt64" 
                typeof<string>,     "GetString"
            ] 
            |> List.map (fun (t, name) -> t, typeof<IDataRecord>.GetMethod(name))                

        static let getValueMethod = typeof<IDataRecord>.GetMethod("GetValue")

        interface IBuilder with
                
            member __.CanBuild(argType: Type): bool = Types.isSimpleType argType
                    
            member __.Build(_, name: string) (prototype: IDataRecord): IRowGetter<'Result> = 
                let ordinal = 
                    try
                        prototype.GetOrdinal(name)
                    with ex -> raise <| Exception(sprintf "Column doesn't exist: %s" name, ex)
                let fieldType = prototype.GetFieldType(ordinal)
                let colGetter = typedColAccessMethods |> List.tryFind (fst >> (=) fieldType) |> Option.map snd |> Option.defaultValue getValueMethod
                let recParam = Expression.Parameter(typeof<IDataRecord>)
                let call = Expression.Call(recParam, colGetter, Expression.Constant(ordinal))
                let convertedCall = 
                    if typeof<'Result> <> call.Type then
                        try
                            Expression.Convert(call, typeof<'Result>) :> Expression
                        with :? InvalidOperationException as ex ->
                            raise <| Exception(sprintf "Column type doesn't match field type: %s (%s -> %s)" name call.Type.Name typeof<'Result>.Name, ex)
                    else
                        call :> Expression
                let getter = Expression.Lambda<Func<IDataRecord, 'Result>>(convertedCall, recParam).Compile()
                { new IRowGetter<'Result> with
                        member __.Get(record: IDataRecord): 'Result = 
                            getter.Invoke(record)
                        member __.IsNull(record: IDataRecord): bool = 
                            record.IsDBNull(ordinal)
                        member this.Create(arg1: IDataRecord): unit = 
                            raise (System.NotImplementedException())
                }

    

    type KeySpecifier<'Primary, 'Foreign>(primary: 'Primary, foreign: 'Foreign) =
            member __.Primary = primary
            member __.Foreign = foreign


open Rows

type RowBuilder(builders: IBuilder seq) = 
    inherit GenericGetters.GenericGetterBuilder<IDataRecord, IDataRecord>(Seq.append builders [ SimpleColumnBuilder()])

    member this.Key<'Result1, 'Result2>(name1: string, name2: string): IDataRecord -> IRowGetter<KeySpecifier<'Result1, 'Result2>> =
        fun (prototype: IDataRecord) ->
            let getter1 = this.CreateGetter<'Result1>(name1, prototype)
            let getter2 = this.CreateGetter<'Result2>(name2, prototype)
            { new IRowGetter<KeySpecifier<'Result1, 'Result2>> with
                member __.IsNull(record: IDataRecord): bool = 
                    getter1.IsNull(record) && getter2.IsNull(record)
                member __.Get(record: IDataRecord): KeySpecifier<'Result1, 'Result2> = 
                    KeySpecifier(getter1.Get(record), getter2.Get(record))
                member __.Create(record: IDataRecord): unit = 
                    getter1.Create(record)
                    getter2.Create(record)
            }

    member __.Key<'Result1, 'Result2>(provider1: IDataRecord -> IRowGetter<'Result1>, provider2: IDataRecord -> IRowGetter<'Result2>): IDataRecord -> IRowGetter<KeySpecifier<'Result1, 'Result2>> = 
        fun (prototype: IDataRecord) ->
            let getter1 = provider1(prototype)
            let getter2 = provider2(prototype)
            { new IRowGetter<KeySpecifier<'Result1, 'Result2>> with
                member __.IsNull(record: IDataRecord): bool = 
                    getter1.IsNull(record) && getter2.IsNull(record)
                member __.Get(record: IDataRecord): KeySpecifier<'Result1, 'Result2> = 
                    KeySpecifier(getter1.Get(record), getter2.Get(record))
                member __.Create(record: IDataRecord): unit = 
                    getter1.Create(record)
                    getter2.Create(record)
            }

    member this.Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, resultName: string) = 
        this.Tuple(this.Key<'Primary, 'Foreign>(primaryName, foreignName), this.Simple<'Result>(resultName))

    member this.Keyed<'Primary, 'Foreign, 'Result>(primaryName: string, foreignName: string, result: IDataRecord -> IRowGetter<'Result>) = 
        this.Tuple(this.Key<'Primary, 'Foreign>(primaryName, foreignName), result)

    member this.Keyed<'Primary, 'Foreign, 'Result>(primary: IDataRecord -> IRowGetter<'Primary>, foreign: IDataRecord -> IRowGetter<'Foreign>, result: IDataRecord -> IRowGetter<'Result>) = 
        this.Tuple(this.Key(primary, foreign), result)

    member this.PK<'Primary, 'Result>(primaryName: string, resultName: string) = 
        this.Tuple(this.Key<'Primary, unit>(primaryName, ""), this.Simple<'Result>(resultName))

    member this.PK<'Primary, 'Result>(primaryName: string, result: IDataRecord -> IRowGetter<'Result>) = 
        this.Tuple(this.Key<'Primary, unit>(primaryName, ""), result)

    member this.PK<'Primary, 'Result>(primary: IDataRecord -> IRowGetter<'Primary>, result: IDataRecord -> IRowGetter<'Result>) = 
        this.Tuple(this.Key<'Primary, unit>(primary, this.Simple<unit>("")), result)

    member this.FK<'Foreign, 'Result>(foreignName: string, resultName: string) = 
        this.Tuple(this.Key<unit, 'Foreign>("", foreignName), this.Simple<'Result>(resultName))

    member this.FK<'Foreign, 'Result>(foreignName: string, result: IDataRecord -> IRowGetter<'Result>) = 
        this.Tuple(this.Key<unit, 'Foreign>("", foreignName), result)

    member this.FK<'Foreign, 'Result>(foreign: IDataRecord -> IRowGetter<'Foreign>, result: IDataRecord -> IRowGetter<'Result>) = 
        this.Tuple(this.Key<unit, 'Foreign>(this.Simple<unit>(""), foreign), result)