package se.liu.ida.jenaext.optplus.sparql.engine.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter1;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.engine.main.QC;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryIterNLJPlusWithOuterLoopOverMaterializedLeft extends QueryIter1
{
    static final boolean JOIN_EXPLAIN = false;

    protected long s_countLHS     = 0L;
    protected long s_countRHS     = 0L;
    protected long s_countResults = 0L;

    protected final Op opRight;
    protected final List<Binding> leftMappings;

    protected QueryIterator itLeft;
    protected Iterator<Binding> itLeftTable;
    protected QueryIterator itCurrentStage;
    protected Binding slot = null;

    public QueryIterNLJPlusWithOuterLoopOverMaterializedLeft( QueryIterator itLeft, Op opRight, ExecutionContext context )
    {
        super(itLeft, context);
        this.opRight = opRight;

        leftMappings = new ArrayList<>();
        this.itLeft = itLeft;
    }

    @Override
    protected boolean hasNextBinding()
    {
        if ( slot == null )
        {
        	if ( itLeft != null )
        	{
        		if ( itLeft.hasNext() )
        		{
        			slot = itLeft.next();
        			leftMappings.add(slot);

        			s_countLHS++;
                    s_countResults++;
        			return true;
        		}

        		itLeft.close();
        		itLeft = null;
        	}

        	itLeftTable = leftMappings.iterator();

            slot = moveToNextBindingOrNull();
            if ( slot == null ) {
                close();
                return false;
            }
        }

        s_countResults++;
        return true;
    }

    @Override
    protected Binding moveToNextBinding()
    {
        Binding r = slot;
        slot = null;
        return r;
    }

    protected Binding moveToNextBindingOrNull()
    {
        if ( isFinished() )
            return null;

        for ( ;; )
        {
            if ( itCurrentStage == null  )
            {
            	if ( ! itLeftTable.hasNext() )
                	return null;

                final Binding mapping = itLeftTable.next();
                itCurrentStage = nextStage(mapping);
            }

            if ( itCurrentStage == null )
            	return null;

            if ( itCurrentStage.hasNext() )
                return itCurrentStage.next();

            itCurrentStage.close();
            itCurrentStage = null;
        }
    }

    protected QueryIterator nextStage( Binding mapping )
    {
        final Op op2 = QC.substitute(opRight, mapping);
        final QueryIterator leftForRight  = QueryIterSingleton.create( mapping, getExecContext() );

        // inner loop
        final QueryIterator currentRight = QC.execute( op2, leftForRight, getExecContext() );

        return currentRight;
    }

    @Override
    protected void closeSubIterator()
    {
        if ( JOIN_EXPLAIN )
        {
            final String x = String.format(
            		getClass().getName() + ": LHS=%d RHS=%d Results=%d",
            		s_countLHS, s_countRHS, s_countResults );
            System.out.println(x);
        }
    }

    @Override
    protected void requestSubCancel() {}

}
