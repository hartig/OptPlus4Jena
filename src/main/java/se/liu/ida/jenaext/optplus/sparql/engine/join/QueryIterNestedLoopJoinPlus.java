package se.liu.ida.jenaext.optplus.sparql.engine.join;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIterConcat;
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.engine.main.iterator.QueryIterOptionalIndex;
import org.apache.jena.sparql.serializer.SerializationContext;

/**
 * This is the OPT+ version of {@link QueryIterOptionalIndex}, which is
 * a nested loops join without materialization. The outer loop iterates
 * over the left input.
 *  
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryIterNestedLoopJoinPlus extends QueryIterRepeatApply
{
    protected final Op opRight;

    public QueryIterNestedLoopJoinPlus( QueryIterator itLeft, Op opRight, ExecutionContext context )
    {
        super(itLeft, context);
        this.opRight = opRight;
    }

    @Override
    protected QueryIterator nextStage( Binding binding )
    {
        final Op op2 = QC.substitute(opRight, binding);
        final QueryIterator leftForOutput = QueryIterSingleton.create( binding, getExecContext() );
        final QueryIterator leftForRight  = QueryIterSingleton.create( binding, getExecContext() );

        // inner loop
        final QueryIterator currentRight = QC.execute( op2, leftForRight, getExecContext() );

        final QueryIterConcat itForStage = new QueryIterConcat( getExecContext() );
        itForStage.add(leftForOutput);
        itForStage.add(currentRight);
        return itForStage;
    }
    
    @Override
    protected void details( IndentedWriter out, SerializationContext sCxt )
    {
        out.println( Lib.className(this) );
        out.incIndent();
        opRight.output(out, sCxt);
        out.decIndent();
    }

}
