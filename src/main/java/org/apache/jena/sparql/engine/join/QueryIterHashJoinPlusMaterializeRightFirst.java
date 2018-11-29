package org.apache.jena.sparql.engine.join;

import java.util.Iterator;

import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.join.AbstractIterHashJoin;
import org.apache.jena.sparql.engine.join.JoinKey;
import org.apache.jena.sparql.engine.join.JoinLib;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryIterHashJoinPlusMaterializeRightFirst extends AbstractIterHashJoin
{
    protected final QueryIterator  itStream;

    protected Binding            currentMappingFromStream = null;
    protected Iterator<Binding>  itCandidates = null;

    public QueryIterHashJoinPlusMaterializeRightFirst( JoinKey joinKey,
                                                       QueryIterator left,
                                                       QueryIterator right,
                                                       ExecutionContext execCxt )
    {
        super(joinKey, right, left, execCxt); // swap left and right to materialize right

        // join key must not be null to avoid that the super
        // class constructor modifies the two input iterators
        if ( joinKey == null )
        	throw new IllegalArgumentException();

        this.itStream = left;
    }

    @Override
    protected Binding moveToNextBinding()
    {
    	s_countResults++;
    	return super.moveToNextBinding();
    }

    @Override
    protected Binding moveToNextBindingOrNull()
    {
        // uses itStream as the stream of incoming mappings

    	// uses currentMappingFromStream as the current mapping from the stream

        // uses itCandidates as the iterator of entries in the
        // hashed table for the current mapping from the stream

        for (;;)
        {
        	// Ensure we are processing a mapping.
            while ( itCandidates == null )
            {
            	if ( currentMappingFromStream == null )
            	{
            		// Move on to the next mapping from the left.
            		if ( ! itStream.hasNext() )
            			return null;

            		s_countScan++;
            		currentMappingFromStream = itStream.next();
            		return currentMappingFromStream;
            	}

                itCandidates = hashTable.getCandidates(currentMappingFromStream);
                if ( itCandidates == null )
                	currentMappingFromStream = null;
            }
 
            if ( ! itCandidates.hasNext() ) {
                itCandidates = null;
                currentMappingFromStream = null;
                continue;
            }

            final Binding r = Algebra.merge( itCandidates.next(), currentMappingFromStream );
            if (r != null)
                return r;
        }
    }

    @Override
    protected void closeSubIterator()
    {
        if ( JoinLib.JOIN_EXPLAIN )
        {
        	final String x = String.format(
                         "HashJoin: LHS=%d RHS=%d Results=%d RightMisses=%d MaxBucket=%d NoKeyBucket=%d",
                         s_countScan, s_countProbe, s_countResults, 
                         hashTable.s_countScanMiss, hashTable.s_maxBucketSize, hashTable.s_noKeyBucketSize);
            System.out.println(x);
        }

        itStream.close();
        hashTable.clear();
    }

    @Override
    protected Binding yieldOneResult( Binding rowCurrentProbe, Binding rowStream, Binding rowResult )
    {
    	return rowResult;
    }

    @Override
    protected Binding noYieldedRows( Binding rowCurrentProbe )
    {
        return null;
    }
    
    @Override
    protected QueryIterator joinFinished()
    {
        return null;
    }

}
