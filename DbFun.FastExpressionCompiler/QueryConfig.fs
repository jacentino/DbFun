namespace DbFun.FastExpressionCompiler

open System.Runtime.CompilerServices
open DbFun.Core.Builders
open DbFun.FastExpressionCompiler.Compilers

type QueryConfigExtensions = 

    [<Extension>]
    static member UseFastExpressionCompiler(config: QueryConfig<'DbKey>) = 
        { config with Compiler = Compiler() }

    [<Extension>]
    static member UseFastExpressionCompiler(config: IDerivedConfig<'DerivedConfig, 'DbKey>) = 
        config.MapCommon(fun common -> common.UseFastExpressionCompiler())




