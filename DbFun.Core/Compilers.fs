namespace DbFun.Core.Builders.Compilers

open System
open System.Linq.Expressions

/// <summary>
/// The expression compiler abstraction.
/// </summary>
type ICompiler = 
    /// <summary>
    /// Compiles expressions to a function.
    /// </summary>
    /// <param name="body">The function body expression.</param>
    /// <param name="args">Function parameters expressions.</param>
    abstract member Compile: body: Expression * [<ParamArray>] args: ParameterExpression[]  -> 'Function when 'Function : not struct 

/// <summary>
/// Default implementation of compiler, based on System.Linq.Expressions compilation.
/// </summary>
type LinqExpressionCompiler() =     
    interface ICompiler with
        member __.Compile(body: Expression, [<ParamArray>] args: ParameterExpression[]): 'Function =                
            Expression.Lambda<'Function>(body, args).Compile()
