namespace DbFun.Core.Builders

open System
open System.Data 
open DbFun.Core

type IParamSetter<'Arg> = GenericSetters.ISetter<IDbCommand, 'Arg>

type IParamSetterProvider = GenericSetters.ISetterProvider<unit, IDbCommand>

type ParamSpecifier<'Arg> = GenericSetters.SetterSpecifier<unit, IDbCommand, 'Arg>

module ParamsImpl = 

    type IBuilder = GenericSetters.IBuilder<unit, IDbCommand>

    type SimpleBuilder() =

        member __.FindOrCreateParam(command: IDbCommand, name: string) = 
            let index = command.Parameters.IndexOf(name)
            if index = -1 then
                let param = command.CreateParameter()
                param.ParameterName <- name
                command.Parameters.Add param |> ignore
                param
            else
                command.Parameters.[index] :?> IDbDataParameter 

        member __.Update(param: IDbDataParameter, value: obj) = 
            if param.Value = null || param.Value = DBNull.Value then
                param.Value <- value
            else
                failwithf "Duplicate parameter definition: %s" param.ParameterName

        member __.GetArtificialValue<'Type>(): obj = 
            if typeof<'Type> = typeof<string> then box ""
            elif typeof<'Type> = typeof<DateTime> then box DateTime.Now
            elif typeof<'Type> = typeof<byte[]> then box [||]
            elif typeof<'Type>.IsClass then null
            else box Unchecked.defaultof<'Type>

        interface IBuilder with

            member __.CanBuild (argType: Type) = Types.isSimpleType(argType)

            member this.Build<'Arg> (name: string, _, ()) = 
                { new IParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, command: IDbCommand) = 
                        let param = this.FindOrCreateParam(command, name)
                        this.Update(param, value)
                    member __.SetNull(command: IDbCommand) = 
                        let param = this.FindOrCreateParam(command, name)
                        this.Update(param, DBNull.Value)
                    member __.SetArtificial(command: IDbCommand) = 
                        let param = this.FindOrCreateParam(command, name)
                        param.Value <- this.GetArtificialValue<'Arg>()
                }

    type SimpleCollectionBuilder() =

        let setValues (name: string, values: 'Element seq, command: IDbCommand): unit =
                let offset = command.Parameters.Count
                for value in values do           
                    let param = command.CreateParameter()
                    param.ParameterName <- sprintf "%s%d" name (command.Parameters.Count - offset)
                    param.Value <- value
                    command.Parameters.Add param |> ignore
                command.CommandText <- command.CommandText.Replace(sprintf "(@%s)" name, sprintf "(%s)" (values |> Seq.mapi (fun i _ -> sprintf "@%s%d" name i) |> String.concat ", "))

        member __.SetSeq(name: string) =
            fun (values: 'Element seq, command: IDbCommand) -> setValues(name, values, command)

        member __.SetList(name: string) =
            fun (values: 'Element list, command: IDbCommand) -> setValues(name, values, command)

        member __.SetArray(name: string) =
            fun (values: 'Element array, command: IDbCommand) -> setValues(name, values, command)

        member __.SetArtificial(setter: IParamSetter<'Element>) = 
            setter.SetArtificial

        interface IBuilder with

            member __.CanBuild (argType: Type) = Types.isCollectionType argType && Types.isSimpleType (Types.getElementType argType)

            member this.Build<'Arg> (name: string, provider: IParamSetterProvider, ()) = 
                let elemType = Types.getElementType(typeof<'Arg>)
                let elemSetter = provider.Setter(elemType, name, ())
                let setValueMethod = 
                    if typeof<'Arg>.IsArray then
                        this.GetType().GetMethod("SetArray").MakeGenericMethod(elemType)
                    elif typedefof<'Arg> = typedefof<list<_>> then
                        this.GetType().GetMethod("SetList").MakeGenericMethod(elemType)
                    else
                        this.GetType().GetMethod("SetSeq").MakeGenericMethod(elemType)
                let valueSetter = setValueMethod.Invoke(this, [| name |]) :?> ('Arg * IDbCommand -> unit)
                let setArtificialMethod = this.GetType().GetMethod("SetArtificial").MakeGenericMethod(elemType)
                let artificialSetter = setArtificialMethod.Invoke(this, [| elemSetter |]) :?> (IDbCommand -> unit)
                { new IParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, command: IDbCommand) = 
                        valueSetter(value, command)
                    member __.SetNull(command: IDbCommand) = 
                        let param = command.CreateParameter()
                        param.ParameterName <- name
                        param.Value <- DBNull.Value
                        command.Parameters.Add param |> ignore
                    member __.SetArtificial(command: IDbCommand) = 
                        artificialSetter(command)
                }

    type BaseSetterProvider = GenericSetters.BaseSetterProvider<unit, IDbCommand>

    type InitialDerivedSetterProvider<'Config> = GenericSetters.InitialDerivedSetterProvider<unit, IDbCommand, 'Config>

    type DerivedSetterProvider<'Config> = GenericSetters.DerivedSetterProvider<unit, IDbCommand, 'Config>

    type UnitBuilder = GenericSetters.UnitBuilder<unit, IDbCommand>

    type SequenceBuilder = GenericSetters.SequenceBuilder<unit, IDbCommand>

    type Converter<'Source, 'Target> = GenericSetters.Converter<unit, IDbCommand, 'Source, 'Target>

    type SeqItemConverter<'Source, 'Target> = GenericSetters.SeqItemConverter<unit, IDbCommand, 'Source, 'Target>

    type EnumConverter<'Underlying> = GenericSetters.EnumConverter<unit, IDbCommand, 'Underlying>

    type EnumSeqConverter<'Underlying> = GenericSetters.EnumSeqConverter<unit, IDbCommand, 'Underlying>

    type UnionSeqBuilder = GenericSetters.UnionSeqBuilder<unit, IDbCommand>

    type UnionBuilder = GenericSetters.UnionBuilder<unit, IDbCommand>

    type OptionBuilder = GenericSetters.OptionBuilder<unit, IDbCommand>

    type RecordBuilder = GenericSetters.RecordBuilder<unit, IDbCommand>

    type TupleBuilder = GenericSetters.TupleBuilder<unit, IDbCommand>

    type Configurator<'Config> = GenericSetters.Configurator<unit, IDbCommand, 'Config>

    let getDefaultBuilders(): IBuilder list = 
        [ SimpleBuilder(); SimpleCollectionBuilder() ] @ GenericSetters.getDefaultBuilders()


/// <summary>
/// Provides methods creating various query parameter builders.
/// </summary>
type Params() = 
    inherit GenericSetters.GenericSetterBuilder<unit, IDbCommand>()

/// <summary>
/// The field-to-parameter mapping override.
/// </summary>
type ParamOverride<'Arg> = GenericSetters.Override<unit, IDbCommand, 'Arg>