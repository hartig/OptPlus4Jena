package org.apache.jena.sparql.engine.join;

import java.util.Iterator;
import java.util.List;

import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.iterator.QueryIter2;
import org.apache.jena.sparql.engine.iterator.QueryIterPeek;
import org.apache.jena.sparql.engine.join.JoinKey;

/**
 * 
 * @author <a href="http://olafhartig.de">Olaf Hartig</a>
 */
public class QueryIterHashJoinPlusMaterializeLeftOnTheFly extends QueryIter2
{
    protected long s_countProbe    = 0L;       // Count of the probe data size
    protected long s_countScan     = 0L;       // Count of the scan data size
    protected long s_countResults  = 0L;       // Overall result size.

    protected final HashProbeTable hashTable;
    protected final QueryIterator  itStream;
    protected QueryIterator        itProbe;

    protected Binding            currentMappingFromStream = null;
    protected Iterator<Binding>  itCandidates = null;

    protected Binding slot = null;

    public QueryIterHashJoinPlusMaterializeLeftOnTheFly( JoinKey joinKey,
                                                         QueryIterator left,
                                                         QueryIterator right,
                                                         ExecutionContext execCxt )
    {
    	super(left, right, execCxt);

        if ( joinKey == null ) {
            final QueryIterPeek pProbe = QueryIterPeek.create(left, execCxt);
            final QueryIterPeek pStream = QueryIterPeek.create(right, execCxt);

            final Binding bLeft = pProbe.peek();
            final Binding bRight = pStream.peek();

            final List<Var> varsLeft = Iter.toList( bLeft.vars() );
            final List<Var> varsRight = Iter.toList( bRight.vars() );
            joinKey = JoinKey.createVarKey(varsLeft, varsRight);
            left = pProbe;
            right = pStream;
        }

        this.itProbe   = left;
        this.itStream  = right;
        this.hashTable = new HashProbeTable(joinKey);
    }

    @Override
    protected boolean hasNextBinding()
    {
        if ( isFinished() ) 
            return false;

        if ( slot == null )
        {
        	if ( itProbe != null )
        	{
        		if ( itProbe.hasNext() ) {
        			s_countProbe++;
        			slot = itProbe.next();
        			hashTable.put(slot);
        			return true;
        		}
        		itProbe.close();
        		itProbe = null;
        	}

            slot = moveToNextBindingOrNull();
            if ( slot == null )
            {
                close();
                return false;
            }
        }

        return true;
    }

    @Override
    protected Binding moveToNextBinding()
    {
        final Binding r = slot;
        slot = null;
		s_countResults++;
        return r;
    }

    protected Binding moveToNextBindingOrNull()
    {
        // uses itStream as the stream of incoming mappings

    	// uses currentMappingFromStream as the current mapping from the stream

        // uses itCandidates as the iterator of entries in the
        // hashed table for the current mapping from the stream

        for (;;)
        {
        	// Ensure we are processing a row. 
            while ( itCandidates == null )
            {
                // Move on to the next row from the right.
                if ( ! itStream.hasNext() )
                    return null;

                s_countScan++;
                currentMappingFromStream = itStream.next();
                itCandidates = hashTable.getCandidates(currentMappingFromStream);
            }

            // Emit one row using the rightRow and the current matched left rows. 
            if ( ! itCandidates.hasNext() ) {
                itCandidates = null;
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
                         s_countProbe, s_countScan, s_countResults, 
                         hashTable.s_countScanMiss, hashTable.s_maxBucketSize, hashTable.s_noKeyBucketSize);
            System.out.println(x);
        }

        itStream.close();
        hashTable.clear();
    }

    @Override
    protected void requestSubCancel() 
    { }

}
