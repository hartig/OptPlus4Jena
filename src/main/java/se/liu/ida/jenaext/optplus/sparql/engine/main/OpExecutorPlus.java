package se.liu.ida.jenaext.optplus.sparql.engine.main;

import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterFilterExpr;
import org.apache.jena.sparql.engine.join.QueryIterHashJoinPlusMaterializeLeftFirst;
import org.apache.jena.sparql.engine.join.QueryIterHashJoinPlusMaterializeLeftOnTheFly;
import org.apache.jena.sparql.engine.join.QueryIterHashJoinPlusMaterializeRightFirst;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprList;

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


    	final String c = execCxt.getContext().getAsString( QueryEnginePlus.classnameOptPlusIterator,
                                                           "QueryIterHashJoinPlusMaterializeLeftOnTheFly" );
        switch( c )
        {
        	case "QueryIterHashJoinPlusMaterializeLeftOnTheFly":
            	return new QueryIterHashJoinPlusMaterializeLeftOnTheFly( null, left, filteredRight, execCxt );

            case "QueryIterHashJoinPlusMaterializeLeftFirst":
            	return new QueryIterHashJoinPlusMaterializeLeftFirst( null, left, filteredRight, execCxt );

            case "QueryIterHashJoinPlusMaterializeRightFirst":
            	return QueryIterHashJoinPlusMaterializeRightFirst.create( left, filteredRight, execCxt );

            case "QueryIterNestedLoopJoinPlusMaterializeLeftOnTheFly":
            	return new QueryIterNestedLoopJoinPlusMaterializeLeftOnTheFly( left, filteredRight, execCxt );

            case "QueryIterNestedLoopJoinPlusMaterializeRightFirst":
            	return new QueryIterNestedLoopJoinPlusMaterializeRightFirst( left, filteredRight, execCxt );
        }

        throw new IllegalArgumentException( "Unexpected classnameOptPlusIterator: " + c );
    }

}
