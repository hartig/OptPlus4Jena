package org.apache.jena.sparql.engine.join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.ext.com.google.common.collect.LinkedListMultimap;
import org.apache.jena.ext.com.google.common.collect.Multimap;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.core.Var;
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
public class QueryIterSubsumptionAwareJoinPlus extends QueryIter1
{
    static final boolean JOIN_EXPLAIN = false;

    protected long s_countLHS     = 0L;
    protected long s_countRHS     = 0L;
    protected long s_countResults = 0L;

    protected final Op opRight;
    protected final List<Binding> leftMappings = new ArrayList<>();

    protected QueryIterator itLeft;
    protected Iterator<Binding> itLeftTable = null;
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
        		itLeftTable = leftMappings.iterator();
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


    static protected class SubsumptionAwareListOfMappings
    {
    	protected final List<Binding>              noKeyBucket = new ArrayList<>();
    	protected final Multimap<Object, Binding>  buckets = LinkedListMultimap.create();
    	protected final Multimap<Binding, Binding> subsumingMappings = LinkedListMultimap.create();

    	protected JoinKey joinKey = null;

    	public void add( Binding mapping )
    	{
    		if ( joinKey == null )
    			joinKey = createJoinKey(mapping);

    		final Object longHash = JoinLib.hash(joinKey, mapping);
            if ( longHash == JoinLib.noKeyHash )
            	noKeyBucket.add(mapping);

            final Collection<Binding> bucket = buckets.get(longHash);
            if ( bucket == null ) {
            	buckets.put(longHash, mapping);
            	return;
            }

//            TODO ...
    	}

    	static public JoinKey createJoinKey( Binding mapping )
    	{
    		final JoinKey.Builder builder = new JoinKey.Builder();

    		final Iterator<Var> it = mapping.vars();
    		while ( it.hasNext() )
    			builder.add( it.next() );

    		return builder.build();
    	}

    } // end of class SubsumptionAwareListOfMappings

}
