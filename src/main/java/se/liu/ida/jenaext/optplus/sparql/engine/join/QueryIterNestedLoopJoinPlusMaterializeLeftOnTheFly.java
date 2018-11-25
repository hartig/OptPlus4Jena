package se.liu.ida.jenaext.optplus.sparql.engine.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter2;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryIterNestedLoopJoinPlusMaterializeLeftOnTheFly extends QueryIter2
{ 
    static final boolean JOIN_EXPLAIN = false;

    protected long s_countLHS     = 0L;
    protected long s_countRHS     = 0L;
    protected long s_countResults = 0L;

    protected QueryIterator itLeft;

    protected final List<Binding> leftMappings;
    protected final QueryIterator itRight;
    protected Binding             currentMappingFromRight = null;
    protected Iterator<Binding>   itLeftMappings          = null;

    protected Binding slot = null;

	public QueryIterNestedLoopJoinPlusMaterializeLeftOnTheFly( QueryIterator left,
                                                               QueryIterator right,
                                                               ExecutionContext cxt )
	{
        super(left, right, cxt);

        leftMappings = new ArrayList<>();
        itLeft = left;
        itRight = right;
    }

    @Override
    protected boolean hasNextBinding()
    {
        if ( slot == null )
        {
        	if ( itLeft != null )
        	{
        		if ( itLeft.hasNext() ) {
        			s_countLHS++;
                    s_countResults++;
        			slot = itLeft.next();
        			leftMappings.add(slot);
        			return true;
        		}

        		itLeft.close();
        		itLeft = null;
        	}

            slot = moveToNextBindingOrNull();
            if ( slot == null ) {
                close();
                return false;
            }
        }
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
            if ( currentMappingFromRight == null )
            {
                if ( itRight.hasNext() )
                {
                    currentMappingFromRight = itRight.next();
                    s_countRHS++;
                    itLeftMappings = leftMappings.iterator();
                }
                else
                    return null;
            }

            while ( itLeftMappings.hasNext() )
            {
                final Binding mappingFromLeft = itLeftMappings.next();
                final Binding r = Algebra.merge(mappingFromLeft, currentMappingFromRight);
                if ( r != null ) {
                    s_countResults++;
                    return r;
                }
            }

            currentMappingFromRight = null;
        }
    }

    @Override
    protected void closeSubIterator()
    {
        if ( JOIN_EXPLAIN )
        {
            final String x = String.format(
            		"InnerLoopJoin: LHS=%d RHS=%d Results=%d",
            		s_countLHS, s_countRHS, s_countResults );
            System.out.println(x);
        }
    }

    @Override
    protected void requestSubCancel() {}

}
