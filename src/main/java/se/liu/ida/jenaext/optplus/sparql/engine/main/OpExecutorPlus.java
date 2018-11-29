package se.liu.ida.jenaext.optplus.sparql.engine.main;

import java.util.List;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterFilterExpr;
import org.apache.jena.sparql.engine.iterator.QueryIterNullIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.join.JoinKey;
import org.apache.jena.sparql.engine.join.QueryIterHashJoinPlusMaterializeLeftFirst;
import org.apache.jena.sparql.engine.join.QueryIterHashJoinPlusMaterializeLeftOnTheFly;
import org.apache.jena.sparql.engine.join.QueryIterHashJoinPlusMaterializeRightFirst;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;

import se.liu.ida.jenaext.optplus.sparql.engine.join.QueryIterNestedLoopJoinPlus;
import se.liu.ida.jenaext.optplus.sparql.engine.join.QueryIterNestedLoopJoinPlusMaterializeLeftOnTheFly;
import se.liu.ida.jenaext.optplus.sparql.engine.join.QueryIterNestedLoopJoinPlusMaterializeRightFirst;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class OpExecutorPlus extends OpExecutor
{
    static final public OpExecutorFactory factory = new OpExecutorFactory() {
		@Override
		public OpExecutor create(ExecutionContext execCxt) {
			return new OpExecutorPlus(execCxt);
		}
	};

	final protected boolean useOptPlusSemantics;

    public OpExecutorPlus( ExecutionContext execCxt )
    {
    	super(execCxt);

    	useOptPlusSemantics = execCxt.getContext().isTrue(QueryEnginePlus.useOptPlusSemantics);
    }

    @Override
    protected QueryIterator exec(Op op, QueryIterator input)
    {
    	return super.exec(op, input);
    }

    @Override
    protected QueryIterator execute( OpLeftJoin opLeftJoin, QueryIterator input )
    {
    	if ( ! useOptPlusSemantics )
    		return super.execute(opLeftJoin, input);

    	final QueryIterator left  = exec( opLeftJoin.getLeft(),  input );
        final QueryIterator right = exec( opLeftJoin.getRight(), root() );

        final ExprList exprs = opLeftJoin.getExprs();

        QueryIterator filteredRight = right;
        if ( exprs != null && ! exprs.isEmpty() ) {
        	for ( Expr expr : exprs )
        		filteredRight = new QueryIterFilterExpr( filteredRight, expr, execCxt );
        }

        return executeOptPlus(left, filteredRight);
    }

    @Override
    protected QueryIterator execute(OpConditional opCondition, QueryIterator input)
    {
    	if ( ! useOptPlusSemantics )
    		return super.execute(opCondition, input);

        final QueryIterator left = exec( opCondition.getLeft(), input );

        final String c = execCxt.getContext().getAsString( QueryEnginePlus.classnameOptPlusIterator,
                                                           "QueryIterHashJoinPlusMaterializeLeftOnTheFly" );
        if ( c.equals("QueryIterNestedLoopJoinPlus") )
        {
            final QueryIterPeek left2  = QueryIterPeek.create(left,  execCxt);
            if ( left2.peek() == null ) {
            	left2.close();
            	return new QueryIterNullIterator(execCxt);
            }
            else
            	return new QueryIterNestedLoopJoinPlus( left2, opCondition.getRight(), execCxt );
        }

        final QueryIterator right = exec( opCondition.getRight(), root() );
        return executeOptPlus(left, right);
    }


    // -------- helpers --------

    protected QueryIterator executeOptPlus( QueryIterator left, QueryIterator right )
    {
        final QueryIterPeek left2  = QueryIterPeek.create(left,  execCxt);
        final QueryIterPeek right2 = QueryIterPeek.create(right, execCxt);

        final Binding bLeft  = left2.peek();
        final Binding bRight = right2.peek();

        if ( bLeft == null ) {
        	left2.close();
        	right2.close();
        	return new QueryIterNullIterator(execCxt);
        }

        if ( bRight == null ) {
        	right2.close();
        	return left2;
        }

    	final List<Var> varsLeft = Iter.toList( bLeft.vars() );
    	final List<Var> varsRight = Iter.toList( bRight.vars() );
    	final JoinKey joinKey = JoinKey.createVarKey(varsLeft, varsRight);

    	final String c = execCxt.getContext().getAsString( QueryEnginePlus.classnameOptPlusIterator,
                                                           "QueryIterHashJoinPlusMaterializeLeftOnTheFly" );
        switch( c )
        {
        	case "QueryIterHashJoinPlusMaterializeLeftOnTheFly":
            	return new QueryIterHashJoinPlusMaterializeLeftOnTheFly( joinKey, left2, right2, execCxt );

            case "QueryIterHashJoinPlusMaterializeLeftFirst":
            	return new QueryIterHashJoinPlusMaterializeLeftFirst( joinKey, left2, right2, execCxt );

            case "QueryIterHashJoinPlusMaterializeRightFirst":
            	return new QueryIterHashJoinPlusMaterializeRightFirst( joinKey, left2, right2, execCxt );

            case "QueryIterNestedLoopJoinPlusMaterializeLeftOnTheFly":
            	return new QueryIterNestedLoopJoinPlusMaterializeLeftOnTheFly( left2, right2, execCxt );

            case "QueryIterNestedLoopJoinPlusMaterializeRightFirst":
            	return new QueryIterNestedLoopJoinPlusMaterializeRightFirst( left2, right2, execCxt );
        }

        throw new IllegalArgumentException( "Unexpected classnameOptPlusIterator: " + c );
    }

}
