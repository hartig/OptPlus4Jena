package org.apache.jena.sparql.engine.join;

import java.util.Iterator;
import java.util.List;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.sparql.engine.iterator.QueryIter1;
import org.apache.jena.sparql.engine.iterator.QueryIterSingleton;
import org.apache.jena.sparql.engine.main.QC;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryIterSubsumptionAwareJoinPlus extends QueryIter1
{
    static final boolean JOIN_EXPLAIN = false;

    protected long s_countLHS     = 0L;
    protected long s_countRHS     = 0L;
    protected long s_countResults = 0L;

    protected final Op opRight;
    protected final SubsumptionAwareCollectionOfMappings leftMappings = new SubsumptionAwareCollectionOfMappings();

    protected QueryIterator itLeft;

    protected Iterator<Binding> itLeftNoKeyMappings = null;
    protected Binding currentNoKeyMapping = null;
    protected QueryIterator itRightForCurrentNoKeyMapping = null;

    protected Iterator<Binding> itLeftSubsumedMappings = null;
    protected Binding currentSubsumedMapping = null;
    protected List<Binding> currentSubsumingMappings = null;
    protected Iterator<Binding> itCurrentSubsumingMapping = null;
    protected QueryIterator itRightForCurrentSubsumedMapping = null;
    protected Binding currentRightMapping = null;

    protected QueryIterator itCurrentStage = null;

    protected Binding slot = null;

    public QueryIterSubsumptionAwareJoinPlus( QueryIterator itLeft, Op opRight, ExecutionContext context )
    {
        super(itLeft, context);

        this.itLeft = itLeft;
        this.opRight = opRight;
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
        		itLeftNoKeyMappings = leftMappings.noKeyMappings();
        		itLeftSubsumedMappings = leftMappings.subsumedMappings();
        	}

        	if ( itLeftNoKeyMappings != null )
        	{
        		slot = moveToNextBindingOrNullForNoKeyBindings();
        		if ( slot != null) {
        	        s_countResults++;
        			return true;
        		}

        		itLeftNoKeyMappings = null;
        	}

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

    protected Binding moveToNextBindingOrNullForNoKeyBindings()
    {
        if ( isFinished() )
            return null;

        for ( ;; )
        {
        	if ( currentNoKeyMapping == null )
        	{
        		if ( ! itLeftNoKeyMappings.hasNext() )
        			return null;

        		currentNoKeyMapping = itLeftNoKeyMappings.next();

        		final QueryIterator leftForRight  = QueryIterSingleton.create( currentNoKeyMapping, getExecContext() );
                itRightForCurrentNoKeyMapping = QC.execute( QC.substitute(opRight,currentNoKeyMapping),
                                                            leftForRight,
                                                            getExecContext() );
        	}

            if ( ! itRightForCurrentNoKeyMapping.hasNext() )
            {
            	itRightForCurrentNoKeyMapping.close();
            	itRightForCurrentNoKeyMapping = null;
            	currentNoKeyMapping = null;
            	continue;
            }

            final Binding b = itRightForCurrentNoKeyMapping.next();

            return merge(currentNoKeyMapping, b);
        }
    }

    protected Binding moveToNextBindingOrNull()
    {
        if ( isFinished() )
            return null;

        for ( ;; )
        {
            if ( currentSubsumedMapping == null  )
            {
            	if ( ! itLeftSubsumedMappings.hasNext() )
                	return null;

            	currentSubsumedMapping = itLeftSubsumedMappings.next();
        		currentSubsumingMappings = leftMappings.subsumingMappings(currentSubsumedMapping);

                final QueryIterator leftForRight  = QueryIterSingleton.create( currentSubsumedMapping, getExecContext() );
                itRightForCurrentSubsumedMapping = QC.execute( QC.substitute(opRight,currentSubsumedMapping),
                                                               leftForRight,
                                                               getExecContext() );
                currentRightMapping = null;
            }

            if ( currentRightMapping == null )
            {
            	if ( ! itRightForCurrentSubsumedMapping.hasNext() )
            	{
            		itRightForCurrentSubsumedMapping.close();
            		itRightForCurrentSubsumedMapping = null;

            		currentSubsumedMapping = null;
            		currentSubsumingMappings = null;
            		itCurrentSubsumingMapping = null;

            		continue;
            	}

        		itCurrentSubsumingMapping = currentSubsumingMappings.iterator();

            	currentRightMapping = itRightForCurrentSubsumedMapping.next();
            	return merge(currentSubsumedMapping, currentRightMapping);
            }

            if ( ! itCurrentSubsumingMapping.hasNext() )
            {
                currentRightMapping = null;
                continue;
            }

            final Binding subsumingMapping = itCurrentSubsumingMapping.next();
            if ( Algebra.compatible(subsumingMapping, currentRightMapping) )
            	return merge(subsumingMapping, currentRightMapping);
        }
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

    /**
     * Merges the two solutions mappings assuming that they are compatible. 
     * Notice, if you want to use a merge method that includes a compatibility
     * check, use {@link Algebra#merge(Binding, Binding)}.  
     */
    static public Binding merge( Binding m1, Binding m2 )
    {
        final BindingMap b = BindingFactory.create(m1);

        final Iterator<Var> it = m2.vars();
        while ( it.hasNext() )
        {
            final Var v = it.next();
            if ( ! m1.contains(v) ) {
                final Node n = m2.get(v);
                b.add(v, n);
            }
        }

        return b;
    }

}
