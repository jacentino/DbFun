namespace DbFun.FastExpressionCompiler.Compilers

open System.Linq.Expressions
open FastExpressionCompiler
open DbFun.Core.Builders.Compilers

/// <summary>
/// The compiler implementation based on FastExpressionCompiler.
/// </summary>
type Compiler() = 
    
    interface ICompiler with
        member __.Compile(body: Expression, args: ParameterExpression array): 'Function when 'Function : not struct = 
            ExpressionCompiler.CompileFast<'Function>(Expression.Lambda(body, args))
