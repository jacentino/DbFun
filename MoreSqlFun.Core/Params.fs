namespace MoreSqlFun.Core.Builders

open System
open System.Data 
open MoreSqlFun.Core

type IParamSetter<'Arg> = GenericSetters.ISetter<IDbCommand, 'Arg>

type IParamSetterProvider = GenericSetters.ISetterProvider<unit, IDbCommand>

module ParamsImpl = 

    type IBuilder = GenericSetters.IBuilder<unit, IDbCommand>

    type SimpleBuilder() =

        member __.GetArtificialValue<'Type>(): obj = 
            if typeof<'Type> = typeof<string> then box ""
            elif typeof<'Type> = typeof<DateTime> then box DateTime.Now
            elif typeof<'Type> = typeof<byte[]> then box [||]
            elif typeof<'Type>.IsClass then null
            else box Unchecked.defaultof<'Type>

        interface IBuilder with

            member __.CanBuild (argType: Type) = Types.isSimpleType(argType)

            member this.Build<'Arg> (_, name: string) () = 
                { new IParamSetter<'Arg> with
                    member __.SetValue (value: 'Arg, command: IDbCommand) = 
                        let param = command.CreateParameter()
                        param.ParameterName <- name
                        param.Value <- value
                        command.Parameters.Add param |> ignore
                    member __.SetNull(command: IDbCommand) = 
                        let param = command.CreateParameter()
                        param.ParameterName <- name
                        param.Value <- DBNull.Value
                        command.Parameters.Add param |> ignore
                    member __.SetArtificial(command: IDbCommand) = 
                        let param = command.CreateParameter()
                        param.ParameterName <- name
                        param.Value <- this.GetArtificialValue<'Arg>()
                        command.Parameters.Add param |> ignore
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

            member this.Build<'Arg> (provider: IParamSetterProvider, name: string) () = 
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

    let getDefaultBuilders(): IBuilder list = 
        [ SimpleBuilder(); SimpleCollectionBuilder() ] @ GenericSetters.getDefaultBuilders()

type Params() = 
    inherit GenericSetters.GenericSetterBuilder<unit, IDbCommand>()

type ParamOverride<'Arg> = GenericSetters.Override<unit, IDbCommand, 'Arg>