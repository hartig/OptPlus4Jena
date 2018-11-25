package se.liu.ida.jenaext.optplus.sparql.engine.join;

import java.util.Iterator;
import java.util.List;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter2;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryIterNestedLoopJoinPlusMaterializeRightFirst extends QueryIter2
{ 
    static final boolean JOIN_EXPLAIN = false;

    protected long s_countLHS     = 0L;
    protected long s_countRHS     = 0L;
    protected long s_countResults = 0L;

    protected final List<Binding> rightMappings;
    protected final QueryIterator itLeft;
    protected Binding             currentMappingFromLeft = null;
    protected Iterator<Binding>   itRightMappings        = null;

    protected Binding slot = null;

	public QueryIterNestedLoopJoinPlusMaterializeRightFirst( QueryIterator left,
                                                             QueryIterator right,
                                                             ExecutionContext cxt )
	{
        super(left, right, cxt);

        rightMappings = Iter.toList(right);
        s_countRHS = rightMappings.size();
        itLeft = left;
    }

    @Override
    protected boolean hasNextBinding()
    {
        if ( slot == null )
        {
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
            if ( currentMappingFromLeft == null )
            {
                if ( ! itLeft.hasNext() )
                    return null;

                currentMappingFromLeft = itLeft.next();
                s_countLHS++;
                itRightMappings = rightMappings.iterator();

                return currentMappingFromLeft;
            }

            while ( itRightMappings.hasNext() )
            {
                final Binding mappingFromRight = itRightMappings.next();
                final Binding r = Algebra.merge(mappingFromRight, currentMappingFromLeft);
                if ( r != null ) {
                    s_countResults++;
                    return r;
                }
            }

            currentMappingFromLeft = null;
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
