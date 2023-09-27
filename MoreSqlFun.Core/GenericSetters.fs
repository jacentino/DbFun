namespace MoreSqlFun.Core.Builders

open System
open System.Reflection
open FSharp.Reflection
open System.Linq.Expressions
open MoreSqlFun.Core

module GenericSetters =

    type ISetter<'DbObject, 'Arg> = 
        abstract member SetValue: 'Arg * 'DbObject -> unit
        abstract member SetNull: 'DbObject -> unit
        abstract member SetArtificial: 'DbObject -> unit

    type ISetterProvider<'DbObject> = 
        abstract member Setter: Type * string -> obj
        abstract member Setter: string -> ISetter<'DbObject, 'Arg>

    type IBuilder<'DbObject> = 
        abstract member CanBuild: Type -> bool
        abstract member Build: ISetterProvider<'DbObject> * string -> ISetter<'DbObject, 'Arg>


    type UnitBuilder<'DbObject>() =

        interface IBuilder<'DbObject> with

            member __.CanBuild (argType: Type) = argType = typeof<unit>

            member __.Build<'Arg> (_: ISetterProvider<'DbObject>, _: string) = 
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (_: 'Arg, _: 'DbObject) = 
                        ()
                    member __.SetNull(_: 'DbObject) = 
                        ()
                    member __.SetArtificial(_: 'DbObject) = 
                        ()
                }

                
    type SequenceBuilder<'DbObject>() =

        interface IBuilder<'DbObject> with

            member __.CanBuild (resType: Type) = Types.isCollectionType resType 

            member __.Build<'Arg> (_: ISetterProvider<'DbObject>, _: string) = 
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (_: 'Arg, _: 'DbObject) = 
                        ()
                    member __.SetNull(_: 'DbObject) = 
                        ()
                    member __.SetArtificial(_: 'DbObject) = 
                        ()
                }


    type Converter<'DbObject, 'Source, 'Target>(convert: 'Source -> 'Target) =

        interface IBuilder<'DbObject> with

            member __.CanBuild (argType: Type) = typeof<'Source>.IsAssignableFrom argType

            member __.Build<'Arg> (provider: ISetterProvider<'DbObject>, name: string) = 
                let assigner = provider.Setter<'Target>(name) 
                let convert' = box convert :?> 'Arg -> 'Target
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (value: 'Arg, command: 'DbObject) = 
                        assigner.SetValue(convert'(value), command)
                    member __.SetNull(command: 'DbObject) = 
                        assigner.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        assigner.SetArtificial(command)
                }

                
    type EnumConverter<'DbObject, 'Underlying>() = 

        interface IBuilder<'DbObject> with

            member __.CanBuild(argType: Type): bool = argType.IsEnum && argType.GetEnumUnderlyingType() = typeof<'Underlying>

            member __.Build(provider: ISetterProvider<'DbObject>, name: string): ISetter<'DbObject, 'Arg> = 
                let assigner = provider.Setter<'Underlying>(name)   
                let enumParam = Expression.Parameter(typeof<'Arg>)
                let convert = Expression.Lambda<Func<'Arg, 'Underlying>>(Expression.Convert(enumParam, typeof<'Underlying>), enumParam).Compile()                    
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (value: 'Arg, command: 'DbObject) = 
                        assigner.SetValue(convert.Invoke(value), command)
                    member __.SetNull(command: 'DbObject) = 
                        assigner.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        assigner.SetArtificial(command)
                }
        

    type AttrEnumConverter<'DbObject>() = 

        interface IBuilder<'DbObject> with

            member __.CanBuild(argType: Type): bool = 
                argType.IsEnum 
                    && 
                argType.GetFields() 
                |> Seq.filter (fun f -> f.IsStatic) 
                |> Seq.forall (fun f -> not (Seq.isEmpty (f.GetCustomAttributes<Models.EnumValueAttribute>())))

            member __.Build(provider: ISetterProvider<'DbObject>, name: string): ISetter<'DbObject, 'Arg> = 
                let assigner = provider.Setter<string>(name)   
                let op1 = Expression.Parameter(typeof<'Arg>)
                let op2 = Expression.Parameter(typeof<'Arg>)
                let eq = Expression.Lambda<Func<'Arg, 'Arg, bool>>(Expression.Equal(op1, op2), op1, op2).Compile()                    
                let underlyingValues = 
                    [ for f in typeof<'Arg>.GetFields() do
                        if f.IsStatic then
                            f.GetValue(null) :?> 'Arg, f.GetCustomAttribute<Models.EnumValueAttribute>().Value
                    ] 
                let convert (x: 'Arg): string = underlyingValues |> List.find (fun (k, _) -> eq.Invoke(x, k)) |> snd
                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (value: 'Arg, command: 'DbObject) = 
                        assigner.SetValue(convert(value), command)
                    member __.SetNull(command: 'DbObject) = 
                        assigner.SetNull(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        assigner.SetArtificial(command)
                }

        
    type OptionBuilder<'DbObject>() =

        member __.GetAssigner(provider: ISetterProvider<'DbObject>, name: string): ISetter<'DbObject, 'Underlying option> = 
            let underlying = provider.Setter<'Underlying>(name)
            { new ISetter<'DbObject, 'Underlying option> with
                member __.SetValue (value: 'Underlying option, command: 'DbObject) = 
                    match value with
                    | Some value -> underlying.SetValue(value, command)
                    | None -> underlying.SetNull(command)
                member __.SetNull(command: 'DbObject) = 
                    underlying.SetNull(command)
                member __.SetArtificial(command: 'DbObject) = 
                    underlying.SetArtificial(command)
            }

        interface IBuilder<'DbObject> with

            member this.CanBuild(argType: Type): bool = Types.isOptionType argType

            member this.Build<'Arg>(provider: ISetterProvider<'DbObject>, name: string): ISetter<'DbObject, 'Arg> = 
                let method = this.GetType().GetMethod("GetAssigner")
                let gmethod = method.MakeGenericMethod(typeof<'Arg>.GetGenericArguments().[0])
                let assigner = gmethod.Invoke(this, [| provider; name |]) 
                assigner :?> ISetter<'DbObject, 'Arg>


    module FieldListAssigner = 
            
        let private callSetValue(assigner: Expression, value: Expression, command: Expression) = 
            Expression.Call(assigner, assigner.Type.GetInterface("ISetter`2").GetMethod("SetValue"), value, command) :> Expression

        let private callSetNull(assigner: Expression, command: Expression) = 
            Expression.Call(assigner, assigner.Type.GetInterface("ISetter`2").GetMethod("SetNull"), command) :> Expression

        let private callSetArtificial(assigner: Expression, command: Expression) = 
            Expression.Call(assigner, assigner.Type.GetInterface("ISetter`2").GetMethod("SetArtificial"), command) :> Expression

        let build(provider: ISetterProvider<'DbObject>, fields: (PropertyInfo * string) seq): ISetter<'DbObject, 'Arg> = 
                    
                let valueParam = Expression.Parameter(typeof<'Arg>)
                let commandParam = Expression.Parameter(typeof<'DbObject>)

                let assignments = 
                    [ for field, name in fields do
                        let fieldValue = Expression.Property(valueParam, field)
                        let assigner = Expression.Constant(provider.Setter(field.PropertyType, name))
                        callSetValue(assigner, fieldValue, commandParam), callSetNull(assigner, commandParam), callSetArtificial(assigner, commandParam)
                    ]

                let valueAssignments = assignments |> Seq.map (fun (setVal, _, _) -> setVal) |> Expression.Block
                let setValueFunction = Expression.Lambda<Action<'Arg, 'DbObject>>(valueAssignments, valueParam, commandParam).Compile()

                let nullAssignments = assignments |> Seq.map (fun (_, setNull, _) -> setNull) |> Expression.Block
                let setNullFunction = Expression.Lambda<Action<'DbObject>>(nullAssignments, commandParam).Compile()

                let artifAssignments = assignments |> Seq.map (fun (_, _, setArtif) -> setArtif) |> Expression.Block
                let setArtifFunction = Expression.Lambda<Action<'DbObject>>(artifAssignments, commandParam).Compile()

                { new ISetter<'DbObject, 'Arg> with
                    member __.SetValue (value: 'Arg, command: 'DbObject) = 
                        setValueFunction.Invoke(value, command)
                    member __.SetNull(command: 'DbObject) = 
                        setNullFunction.Invoke(command)
                    member __.SetArtificial(command: 'DbObject) = 
                        setArtifFunction.Invoke(command)
                }


    type RecordBuilder<'DbObject>() = 
            
        interface IBuilder<'DbObject> with

            member this.CanBuild(argType: Type): bool = FSharpType.IsRecord(argType)

            member this.Build(provider: ISetterProvider<'DbObject>, name: string): ISetter<'DbObject, 'Arg> = 
                let fields = FSharpType.GetRecordFields typeof<'Arg> |> Array.map (fun f -> f, f.Name)
                FieldListAssigner.build(provider, fields)                    


    type TupleBuilder<'DbObject>() = 

        interface IBuilder<'DbObject> with

            member __.CanBuild(argType: Type): bool = FSharpType.IsTuple argType

            member __.Build(provider: ISetterProvider<'DbObject>, name: string): ISetter<'DbObject, 'Arg> =                     
                let fields = typeof<'Arg>.GetProperties() |> Array.mapi (fun i f -> f, sprintf "%s%d" name (i + 1))
                FieldListAssigner.build(provider, fields)  


    type GenericSetterBuilder<'DbObject>(builders: IBuilder<'DbObject> seq) = 

        let builders = 
            [
                yield! builders
                UnitBuilder<'DbObject>()
                SequenceBuilder<'DbObject>()
                RecordBuilder<'DbObject>()
                TupleBuilder<'DbObject>()
                OptionBuilder<'DbObject>()
                Converter<'DbObject, _, _>(fun (dtOnly: DateOnly) -> dtOnly.ToDateTime(TimeOnly.MinValue))
                Converter<'DbObject, _, _>(fun (tmOnly: TimeOnly) -> tmOnly.ToTimeSpan())
                AttrEnumConverter<'DbObject>()
                EnumConverter<'DbObject, char>()
                EnumConverter<'DbObject, int>()
            ]

        member this.Unit = this.GetSetter<unit>("")

        member this.Simple<'Arg> (name: string) = this.GetSetter<'Arg>(name)

        member __.Optional<'Arg> (underlying: ISetter<'DbObject, 'Arg>) = 
                { new ISetter<'DbObject, 'Arg option> with
                member __.SetValue (value: 'Arg option, command: 'DbObject) = 
                    match value with
                    | Some value -> underlying.SetValue(value, command)
                    | None -> underlying.SetNull(command)
                member __.SetNull(command: 'DbObject) = 
                    underlying.SetNull(command)
                member __.SetArtificial(command: 'DbObject) = 
                    underlying.SetArtificial(command)
                }

        member __.Tuple<'Arg1, 'Arg2>(item1: ISetter<'DbObject, 'Arg1>, item2: ISetter<'DbObject, 'Arg2>) = 
            { new ISetter<'DbObject, 'Arg1 * 'Arg2> with
                member __.SetValue((val1: 'Arg1, val2: 'Arg2), command: 'DbObject) =
                    item1.SetValue(val1, command)
                    item2.SetValue(val2, command)
                member __.SetNull(command: 'DbObject) = 
                    item1.SetNull(command)
                    item2.SetNull(command)
                member __.SetArtificial(command: 'DbObject) = 
                    item1.SetArtificial(command)
                    item2.SetArtificial(command)
            }
        
        member this.Tuple<'Arg1, 'Arg2>(name1: string, name2: string) = 
            let item1 = this.GetSetter<'Arg1>(name1) 
            let item2 = this.GetSetter<'Arg2>(name2)
            { new ISetter<'DbObject, 'Arg1 * 'Arg2> with
                member __.SetValue((val1: 'Arg1, val2: 'Arg2), command: 'DbObject) =
                    item1.SetValue(val1, command)
                    item2.SetValue(val2, command)
                member __.SetNull(command: 'DbObject) = 
                    item1.SetNull(command)
                    item2.SetNull(command)
                member __.SetArtificial(command: 'DbObject) = 
                    item1.SetArtificial(command)
                    item2.SetArtificial(command)
            }

        member __.Tuple<'Arg1, 'Arg2, 'Arg3>(item1: ISetter<'DbObject, 'Arg1>, item2: ISetter<'DbObject, 'Arg2>, item3: ISetter<'DbObject, 'Arg3>) = 
            { new ISetter<'DbObject, 'Arg1 * 'Arg2 * 'Arg3> with
                member __.SetValue((val1: 'Arg1, val2: 'Arg2, val3: 'Arg3), command: 'DbObject) =
                    item1.SetValue(val1, command)
                    item2.SetValue(val2, command)
                    item3.SetValue(val3, command)
                member __.SetNull(command: 'DbObject) = 
                    item1.SetNull(command)
                    item2.SetNull(command)
                    item3.SetNull(command)
                member __.SetArtificial(command: 'DbObject) = 
                    item1.SetArtificial(command)
                    item2.SetArtificial(command)
                    item3.SetArtificial(command)
            }
        
        member this.Tuple<'Arg1, 'Arg2, 'Arg3>(name1: string, name2: string, name3: string) = 
            let item1 = this.GetSetter<'Arg1>(name1) 
            let item2 = this.GetSetter<'Arg2>(name2)
            let item3 = this.GetSetter<'Arg3>(name3)
            { new ISetter<'DbObject, 'Arg1 * 'Arg2 * 'Arg3> with
                member __.SetValue((val1: 'Arg1, val2: 'Arg2, val3: 'Arg3), command: 'DbObject) =
                    item1.SetValue(val1, command)
                    item2.SetValue(val2, command)
                    item3.SetValue(val3, command)
                member __.SetNull(command: 'DbObject) = 
                    item1.SetNull(command)
                    item2.SetNull(command)
                    item3.SetNull(command)
                member __.SetArtificial(command: 'DbObject) = 
                    item1.SetArtificial(command)
                    item2.SetArtificial(command)
                    item3.SetArtificial(command)
            }

        member this.GetSetter(argType: Type, name: string): obj = 
            let method = this.GetType().GetMethods() |> Seq.find (fun m -> m.Name = "GetSetter" && m.IsGenericMethod && m.GetGenericArguments().Length = 1)
            let gmethod = method.MakeGenericMethod(argType)
            gmethod.Invoke(this, [| name |])

        member this.GetSetter<'Arg>(name: string): ISetter<'DbObject, 'Arg> = 
            match builders |> Seq.tryFind (fun b -> b.CanBuild typeof<'Arg>) with
            | Some builder -> builder.Build(this, name)
            | None -> failwithf "Could not found param builder for type: %A" typeof<'Arg>

        member this.Optional<'Arg> (name: string) = this.GetSetter<'Arg option>(name)

        member this.Record<'Arg>(prefix: string) = this.GetSetter<'Arg>(prefix)

        interface ISetterProvider<'DbObject> with
            member this.Setter<'Arg>(name: string): ISetter<'DbObject, 'Arg> = 
                this.GetSetter<'Arg>(name)
            member this.Setter(argType: Type, name: string): obj = 
                this.GetSetter(argType, name)
            